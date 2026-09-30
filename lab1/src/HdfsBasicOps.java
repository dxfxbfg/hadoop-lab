import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HdfsBasicOps {

    static final String HDFS_URI = "hdfs://localhost:9000";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("用法: HdfsBasicOps <操作编号> [参数...]");
            System.out.println("1  上传文件（已存在时询问追加或覆盖）");
            System.out.println("2  下载文件（本地同名时自动重命名）");
            System.out.println("3  输出文件内容到终端");
            System.out.println("4  显示文件的权限/大小/创建时间/路径");
            System.out.println("5  递归显示目录下所有文件的信息");
            System.out.println("6  创建或删除文件（目录不存在自动创建）");
            System.out.println("7  创建或删除目录（非空目录不删除）");
            System.out.println("8  向文件追加内容（指定开头或结尾）");
            System.out.println("9  删除文件");
            System.out.println("10 移动文件");
            return;
        }

        Configuration conf = new Configuration();
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        FileSystem fs = FileSystem.get(URI.create(HDFS_URI), conf);

        switch (args[0]) {
            case "1":  upload(fs, args[1], args[2]); break;
            case "2":  download(fs, args[1], args[2]); break;
            case "3":  cat(fs, args[1]); break;
            case "4":  stat(fs, args[1]); break;
            case "5":  listRecursive(fs, args[1]); break;
            case "6":  fileCreateDelete(fs, args[1], args[2]); break;
            case "7":  dirCreateDelete(fs, args[1], args[2]); break;
            case "8":  append(fs, args[1], args[2], args[3]); break;
            case "9":  delete(fs, args[1]); break;
            case "10": move(fs, args[1], args[2]); break;
            default:   System.out.println("未知操作: " + args[0]);
        }

        fs.close();
    }

    static void upload(FileSystem fs, String local, String remote) throws IOException {
        Path dst = new Path(remote);
        if (fs.exists(dst)) {
            System.out.print("HDFS 中已存在 " + remote + "，追加(a)还是覆盖(o)？");
            String choice = new Scanner(System.in).nextLine().trim();
            if (choice.equalsIgnoreCase("a")) {
                try (FSDataOutputStream out = fs.append(dst);
                     InputStream in = new java.io.FileInputStream(local)) {
                    byte[] buf = new byte[4096];
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        out.write(buf, 0, n);
                    }
                }
                System.out.println("已追加到 " + remote);
            } else if (choice.equalsIgnoreCase("o")) {
                fs.delete(dst, false);
                fs.copyFromLocalFile(new Path(local), dst);
                System.out.println("已覆盖 " + remote);
            } else {
                System.out.println("未识别输入，操作取消");
            }
        } else {
            fs.copyFromLocalFile(new Path(local), dst);
            System.out.println("已上传到 " + remote);
        }
    }

    static void download(FileSystem fs, String remote, String local) throws IOException {
        Path localPath = new Path(local);
        java.io.File f = new java.io.File(localPath.toUri().getPath());
        if (f.exists()) {
            String name = f.getName();
            String dir = f.getParent();
            int dot = name.lastIndexOf('.');
            String base = dot > 0 ? name.substring(0, dot) : name;
            String ext = dot > 0 ? name.substring(dot) : "";
            int i = 1;
            while (f.exists()) {
                f = new java.io.File(dir, base + "_" + i + ext);
                i++;
            }
            System.out.println("本地已存在同名文件，改存为 " + f.getPath());
        }
        fs.copyToLocalFile(new Path(remote), new Path(f.getPath()));
        System.out.println("已下载到 " + f.getPath());
    }

    static void cat(FileSystem fs, String remote) throws IOException {
        Path p = new Path(remote);
        if (!fs.exists(p)) {
            System.out.println("文件不存在: " + remote);
            return;
        }
        try (FSDataInputStream in = fs.open(p);
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }
    }

    static void stat(FileSystem fs, String remote) throws IOException {
        Path p = new Path(remote);
        if (!fs.exists(p)) {
            System.out.println("路径不存在: " + remote);
            return;
        }
        FileStatus s = fs.getFileStatus(p);
        System.out.println("路径    : " + s.getPath());
        System.out.println("权限    : " + s.getPermission());
        System.out.println("大小    : " + s.getLen() + " 字节");
        System.out.println("创建时间: " + fmt(s.getModificationTime()));
    }

    static void listRecursive(FileSystem fs, String remote) throws IOException {
        Path p = new Path(remote);
        if (!fs.exists(p) || !fs.isDirectory(p)) {
            System.out.println("目录不存在: " + remote);
            return;
        }
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, true);
        while (it.hasNext()) {
            FileStatus s = it.next();
            System.out.println(s.getPermission() + "\t" + s.getLen() + "\t"
                    + fmt(s.getModificationTime()) + "\t" + s.getPath());
        }
    }

    static void fileCreateDelete(FileSystem fs, String remote, String action) throws IOException {
        Path p = new Path(remote);
        if (action.equalsIgnoreCase("create")) {
            if (fs.exists(p)) {
                System.out.println("文件已存在: " + remote);
                return;
            }
            fs.create(p).close();
            System.out.println("已创建文件 " + remote);
        } else if (action.equalsIgnoreCase("delete")) {
            if (fs.delete(p, false)) {
                System.out.println("已删除文件 " + remote);
            } else {
                System.out.println("删除失败，文件不存在: " + remote);
            }
        } else {
            System.out.println("action 应为 create 或 delete");
        }
    }

    static void dirCreateDelete(FileSystem fs, String remote, String action) throws IOException {
        Path p = new Path(remote);
        if (action.equalsIgnoreCase("create")) {
            if (fs.mkdirs(p)) {
                System.out.println("已创建目录 " + remote + "（父目录已自动创建）");
            } else {
                System.out.println("目录已存在: " + remote);
            }
        } else if (action.equalsIgnoreCase("delete")) {
            FileStatus[] children = fs.exists(p) ? fs.listStatus(p) : null;
            if (children == null) {
                System.out.println("目录不存在: " + remote);
                return;
            }
            if (children.length > 0) {
                System.out.println("目录非空，不执行删除: " + remote);
                return;
            }
            if (fs.delete(p, false)) {
                System.out.println("已删除空目录 " + remote);
            }
        } else {
            System.out.println("action 应为 create 或 delete");
        }
    }

    static void append(FileSystem fs, String remote, String content, String pos) throws IOException {
        Path p = new Path(remote);
        if (!fs.exists(p)) {
            System.out.println("文件不存在: " + remote);
            return;
        }
        if (pos.equalsIgnoreCase("end")) {
            try (FSDataOutputStream out = fs.append(p)) {
                out.write(content.getBytes("UTF-8"));
                out.write('\n');
            }
            System.out.println("已追加到文件结尾");
        } else if (pos.equalsIgnoreCase("begin")) {
            String old = readAll(fs, p);
            fs.delete(p, false);
            try (FSDataOutputStream out = fs.create(p)) {
                out.write(content.getBytes("UTF-8"));
                out.write('\n');
                out.write(old.getBytes("UTF-8"));
            }
            System.out.println("已追加到文件开头");
        } else {
            System.out.println("位置参数应为 begin 或 end");
        }
    }

    static void delete(FileSystem fs, String remote) throws IOException {
        Path p = new Path(remote);
        if (fs.delete(p, false)) {
            System.out.println("已删除 " + remote);
        } else {
            System.out.println("删除失败，文件不存在: " + remote);
        }
    }

    static void move(FileSystem fs, String src, String dst) throws IOException {
        Path s = new Path(src);
        Path d = new Path(dst);
        if (!fs.exists(s)) {
            System.out.println("源路径不存在: " + src);
            return;
        }
        if (fs.rename(s, d)) {
            System.out.println("已从 " + src + " 移动到 " + dst);
        } else {
            System.out.println("移动失败");
        }
    }

    static String readAll(FileSystem fs, Path p) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (FSDataInputStream in = fs.open(p);
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    static String fmt(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(ts));
    }
}
