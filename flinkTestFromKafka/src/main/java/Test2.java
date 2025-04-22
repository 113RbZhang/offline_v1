/**
 * @Package PACKAGE_NAME.Test2
 * @Author runbo.zhang
 * @Date 2025/4/21 15:56
 * @description:
 */
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class Test2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 获取 log4j.properties 的 URL
        URL resourceUrl = Test2.class.getClassLoader().getResource("log4j.properties");
        if (resourceUrl != null) {
            System.out.println("log4j.properties 文件地址: " + resourceUrl.toString());
            try (InputStream inputStream = resourceUrl.openStream()) {
                if (inputStream != null) {
                    System.out.println("成功找到 log4j.properties 文件");
                    properties.load(inputStream);
                    for (String key : properties.stringPropertyNames()) {
                        String value = properties.getProperty(key);
                        System.out.println(key + " = " + value);
                    }
                } else {
                    System.err.println("未找到 log4j.properties 文件");
                }
            } catch (IOException e) {
                System.err.println("读取 log4j.properties 文件时出错: " + e.getMessage());
            }
        } else { System.err.println("未找到 log4j.properties 文件的 URL");
        }

        }
}
