package com.maxaira;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
 
import okhttp3.*;
 
public class OkHttpSocks5Test {
 
    String run(String url) throws IOException {
        // define your proxy details
        String proxyHost = "103.129.161.230";
        int proxyPort = 7778;
 
        // create a OkHttpClient builder instance and configure it to use the proxy
        OkHttpClient client = new OkHttpClient.Builder()
            .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort)))
            .proxyAuthenticator(new Authenticator() {
                @Override public Request authenticate(Route route, Response response) {
                    String credential = Credentials.basic("i4z5B2Y9S1I4", "s9h3y8c0P5F7");
                    return response.request().newBuilder()
                            .header("Proxy-Authorization", credential)
                            .build();
                }
            })
            .build();
 
        // create a request with the provided URL
        Request request = new Request.Builder()
            .url(url)
            .build();
        // execute the request and obtain the response
        try (Response response = client.newCall(request).execute()) {
            // return the response body as a string
            return response.body().string();
        }
    }
 
    public static void main(String[] args) throws IOException {
        // create an instance of the Main class
        OkHttpSocks5Test example = new OkHttpSocks5Test();
        // make a GET request to the specified URL and print the response
        // String response = example.run("https://httpbin.io/ip");
        String response = example.run("https://fapi.binance.com/fapi/v1/klines?symbol=ETHUSDT&interval=1h&startTime=1694508800000&endTime=1694595199000&limit=10");
        System.out.println(response);
    }
}

