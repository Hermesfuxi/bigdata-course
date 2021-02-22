package bigdata.hermesfuxi.flume.interceptor.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;

public class MD5Utils {
    public static String encrypt(String value){
        if(StringUtils.isEmpty(value)){
            return "";
        }
        return DigestUtils.md5Hex(value);
    }
}
