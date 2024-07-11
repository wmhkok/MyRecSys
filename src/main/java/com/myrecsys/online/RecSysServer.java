package com.myrecsys.online;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;

import com.myrecsys.online.service.*;
import com.myrecsys.online.datamanager.DataManager;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

public class RecSysServer {
    public static void main(String[] args) throws Exception {
        new RecSysServer().run();
    }

    private static final int DEAFULT_PORT = 8010;

    public void run() throws Exception {
        int port = DEAFULT_PORT;
        InetSocketAddress inetAddress = new InetSocketAddress(port);
        Server server = new Server(inetAddress);

        URL webRootLocation = this.getClass().getResource("/webroot/index.html");
        if (webRootLocation == null) {
            throw new IllegalStateException("Unable to determine webroot URL location");
        }
        
        // ルートページをindex.htmlに設定
        URI webRootUri = URI.create(webRootLocation.toURI().toASCIIString().replaceFirst("/index.html$", "/"));
        System.out.printf("Web Root URI: %s%n", webRootUri.getPath());

        // 必要なデータをDataManagerにロードする
        DataManager.getInstance().loadData(webRootUri.getPath() + "data/sampledata/movies.csv",
                webRootUri.getPath() + "data/links.csv",
                webRootUri.getPath() + "data/ratings.csv",
                webRootUri.getPath() + "data/item2VecEmb.txt",
                webRootUri.getPath() + "data/userEmb.txt",
                "i2vEmb", "uEmb");
        
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.setBaseResource(Resource.newResource(webRootUri));
        context.setWelcomeFiles(new String[] { "index.html" });
        context.getMimeTypes().addMimeMapping("txt","text/plain;charset=utf-8");

        context.addServlet(DefaultServlet.class,"/");
        context.addServlet(new ServletHolder(new MovieService()), "/getmovie");
        context.addServlet(new ServletHolder(new UserService()), "/getuser");
        context.addServlet(new ServletHolder(new SimilarMovieService()), "/getsimilarmovie");
        context.addServlet(new ServletHolder(new RecommendationService()), "/getrecommendation");
        context.addServlet(new ServletHolder(new RecForYouService()), "/getrecforyou");

        
        server.setHandler(context);
        System.out.println("RecSys Server has started.");
        server.start();
        server.join();
    }
}
