package com.yunzhi.mq.rocketmq.base.consumer;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author zxb
 * @ClassName Test
 * @Description TODO
 * @Date 2021/12/24 16:26
 * @Version 1.0
 */
public class TestTest {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String str = "我爱祖国";
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        System.out.println(new String(bytes,StandardCharsets.ISO_8859_1));
    }
}
