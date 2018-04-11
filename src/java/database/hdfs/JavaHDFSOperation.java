package database.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by engry on 2018/4/11.
 */
public class JavaHDFSOperation {
    private String master_name = "master";
    private String uri = "hdfs://" + master_name + ":9000";
    /**
     * hdfs exists in local host.
     * */
    public FileSystem getHDFSbyLocalConf() {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        conf.set("fs.defaultFS", uri);
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }

    /**
     * hdfs exists in remote hosts.
     */
    public FileSystem getHDFSbyURI() {
        Configuration conf = new Configuration();
        URI hdfsURI = null;
        try {
            hdfsURI = new URI(uri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        FileSystem fs = null;
        try {
            fs = FileSystem.get(hdfsURI, conf, master_name);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public FileSystem getHDFSbyAddResource() {
        Configuration conf = new Configuration();
        conf.addResource("/core-site.xml");
        conf.addResource("/hdfs-site.xml");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public boolean createHDFSPath(FileSystem fs, String createPath) {
        boolean b = false;
        Path path = new Path(createPath);

        try {
            b = fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return b;
    }

    public boolean deleteHDFSPath(FileSystem fs, String deletePath, boolean recursive) {
        boolean b = false;
        Path path = new Path(deletePath);
        try {
            b = fs.delete(path, recursive);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return b;
    }

    public boolean renameHDFSPath(FileSystem fs, String oldName, String newName) {
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean b = false;
        try {
            fs.rename(oldPath, newPath);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return b;
    }

    public void recursiveHDFSPath(FileSystem fs, Path listPath) {
        Set<String> paths = new HashSet<String>();
        recursiveHDFSPath(fs, listPath, paths);
    }

    public void recursiveHDFSPath(FileSystem fs, Path listPath, Set<String> set) {
        FileStatus[] files = null;
        try {
            files = fs.listStatus(listPath);
            if (files.length == 0)
                set.add(listPath.toUri().getPath());
            else {
                for (FileStatus f: files) {
                    if (f.isFile())
                        set.add(f.getPath().toUri().getPath());
                    else
                        recursiveHDFSPath(fs, f.getPath(), set);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void check(FileSystem fs, String pathStr) {
        Path path = new Path(pathStr);
        try {
            if (fs.exists(path))
                System.out.println("path don't exist !");
            else if (fs.isDirectory(path))
                System.out.println("this is a directory !");
            else if (fs.isFile(path))
                System.out.println("this is a file !");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void showAllConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", uri);
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
    }

    public void getFileFromHDFS(FileSystem fs, String local, String hdfs) {
        Path localPath = new Path(local);
        Path hdfsPath = new Path(hdfs);
        try {
            fs.copyToLocalFile(hdfsPath, localPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putFileToHDFS(FileSystem fs, String hdfs, String local, boolean overwrite) {
        Path localPath = new Path(local);
        Path hdfsPath = new Path(hdfs);
        try {
            if (!overwrite && fs.exists(hdfsPath)) {
                System.out.println("path exists");
                return;
            }
            fs.copyFromLocalFile(localPath, hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void copyFileBetweenHDFS(FileSystem fs, String input, String output) {
        Path inPath = new Path(input);
        Path outPath = new Path(output);
        FSDataInputStream inStream = null;
        FSDataOutputStream outStream = null;
        try {
            inStream = fs.open(inPath);
            outStream = fs.create(outPath);
            IOUtils.copyBytes(inStream, outStream, 1024 * 1024*64, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                outStream.close();
                inStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
