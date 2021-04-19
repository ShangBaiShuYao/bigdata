package com.shangbaishuyao;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * @author administrator
 *
 */
public class NetworkMonitor implements Runnable {
    private String m_strUrl = "192.168.1.102";
    private static boolean m_bNetworkAvailable = false;

    /**
     *
     */
    public NetworkMonitor() {
        // TODO:
    }

    /**
     * @param strUrl
     */
    public NetworkMonitor(String strUrl) {
        this.m_strUrl = strUrl;
    }

    /**
     * @return
     */
    public static boolean isNetworkAvailable() {
        return m_bNetworkAvailable;
    }

    /* (non-Javadoc)
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        while (true) {
            try {
                InetAddress inetAddress = InetAddress.getByName(m_strUrl);
                m_bNetworkAvailable = inetAddress.isReachable(5000);		//测试是否可以达到该地址

                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
                m_bNetworkAvailable = false;
            } catch(UnknownHostException e) {
                // System.err.println("连接失败");
                m_bNetworkAvailable = false;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
                m_bNetworkAvailable = false;
            }
        }
    }

    public void isAddressAvailable(String ip) {
        try {
            InetAddress address = InetAddress.getByName(ip);	//ping this IP
            if (address instanceof java.net.Inet4Address) {
                System.out.println(ip + " is ipv4 address");
            } else if (address instanceof java.net.Inet6Address) {
                System.out.println(ip + " is ipv6 address");
            } else {
                System.out.println(ip + " is unrecongized");
            }

            if (address.isReachable(5000)) {
                System.out.println("SUCCESS - ping " + ip + " with no interface specified");
            } else {
                System.out.println("FAILURE - ping " + ip + " with no interface specified");
            }

            System.out.println("\n-------Trying different interfaces--------");
            Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
            while (netInterfaces.hasMoreElements()) {
                NetworkInterface ni = netInterfaces.nextElement();
                System.out.println( "Checking interface, DisplayName:" + ni.getDisplayName() + ", Name:" + ni.getName());
                if(address.isReachable(ni, 0, 5000)){
                    System.out.println("SUCCESS - ping " + ip);
                } else {
                    System.out.println("FAILURE - ping " + ip);
                }
                Enumeration<InetAddress> ips = ni.getInetAddresses();
                while(ips.hasMoreElements()) {
                    System.out.println("IP: " + ips.nextElement().getHostAddress());
                }
                System.out.println("-------------------------------------------"); }
        } catch (Exception e) {
            System.out.println("error occurs.");
            e.printStackTrace();
        }
    }
}