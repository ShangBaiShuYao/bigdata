package com.shangbaishuyao;


public class Startup {
    public static void main(String[] args) {
        Startup startup = new Startup();
        startup.startNetworkMonitor();

        for (int i = 0; i < 60; i++) {
            System.out.println("network[" + i + 1 +"]: " + NetworkMonitor.isNetworkAvailable());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void startNetworkMonitor() {
        NetworkMonitor networkMonitor = new NetworkMonitor();
        networkMonitor.isAddressAvailable("192.168.1.102");
        Thread thread = new Thread(networkMonitor);
        thread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}