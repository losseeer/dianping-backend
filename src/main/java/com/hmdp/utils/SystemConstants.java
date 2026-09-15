package com.hmdp.utils;

public class SystemConstants {
    // 图片上传根目录：用户主目录下 hmdp-imgs，跨平台可用（原教程写死的 Windows 路径 D:\... 无法在 mac/Linux 运行）
    // /imgs/** 与 /blogs/** 的静态映射见 MvcConfig.addResourceHandlers
    public static final String IMAGE_UPLOAD_DIR = System.getProperty("user.home") + "/hmdp-imgs";
    public static final String USER_NICK_NAME_PREFIX = "user_";
    public static final int DEFAULT_PAGE_SIZE = 5;
    public static final int MAX_PAGE_SIZE = 10;
}
