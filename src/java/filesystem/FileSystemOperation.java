package filesystem;

import java.io.File;

/**
 * operate native filesystem
 */
public class FileSystemOperation {
    public void deleteFile(String delPath) {
        File curFile = new File(delPath);
        if (!curFile.exists()) {
            return;
        }
        if (curFile.isFile()) {
            curFile.delete();
            System.out.println("delete file: " + delPath);
        } else {
            String[] filelist = curFile.list();
            for (String path: filelist) {
                deleteFile(delPath + "/" + path);
            }
            curFile.delete();
        }

    }
//    public static void main(String[] args) {
//        FileSystemOperation test = new FileSystemOperation();
//        test.deleteFile("data/test");
//    }
}
