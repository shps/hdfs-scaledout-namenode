package se.kth.kthfsdashboard.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class WebCommunication {

   public enum Type {

      STDOUT, STDERR, DO
   }
   
   private static String USERNAME = "kthfsagent@sics.se";
   private static String PASSWORD = "kthfsagent";
   private static int PORT = 8090;
   private static int LOG_LINES = 50;
   private static String NOT_AVAILABLE = "Not available.";
   private String hostname;
   private String kthfsInstance;
   private String service;
   
   private static final Logger logger = Logger.getLogger(WebCommunication.class.getName());

   public WebCommunication(String hostname, String kthfsInstance, String service) {
      this.hostname = hostname;
      this.kthfsInstance = kthfsInstance;
      this.service = service;
   }

   public String getStdOut() {
      return getLog("stdout");
   }

   public String getStdErr() {
      return getLog("stderr");
   }

   private String getLog(String logType) {
      
      String log = NOT_AVAILABLE;
      String path = "/log/" + kthfsInstance + "/" + service + "/" + logType + "/" + LOG_LINES;
      String url = baseUrl(hostname) + path;
      try {
         ClientResponse response = getWebResource(url);
         if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
            log = response.getEntity(String.class);
            log = log.replaceAll("\n", "<br>");
         }
      } catch (Exception ex) {
         logger.log(Level.SEVERE, null, ex);
      }
      return log;
   }

   public String getConfig() {

      String conf = NOT_AVAILABLE;
      String path = "/config/" + kthfsInstance + "/" + service;
      String url = baseUrl(hostname) + path;
      try {
         ClientResponse response = getWebResource(url);
         if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
            conf = response.getEntity(String.class);
         }
      } catch (Exception e) {
         logger.log(Level.SEVERE, null, e);
      }
      return conf;
   }

   public ClientResponse doCommand(String command) throws Exception {

      String path = "/do/" + kthfsInstance + "/" + service + "/" + command;
      String url = baseUrl(hostname) + path;
      return getWebResource(url);
   }

   private static String baseUrl(String hostname) {

      return "https://" + hostname + ":" + PORT;
   }

   private ClientResponse getWebResource(String url) throws Exception {

      disableCertificateValidation();
      Client client = Client.create();
      WebResource webResource = client.resource(url);
      MultivaluedMap params = new MultivaluedMapImpl();
      params.add("username", USERNAME);
      params.add("password", PASSWORD);

      return webResource.queryParams(params).get(ClientResponse.class);
   }

   private static void disableCertificateValidation() {
      // Create a trust manager that does not validate certificate chains
      TrustManager[] trustAllCerts = new TrustManager[]{
         new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
               return new X509Certificate[0];
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
         }};

      // Ignore differences between given hostname and certificate hostname
      HostnameVerifier hv = new HostnameVerifier() {
         public boolean verify(String hostname, SSLSession session) {
            return true;
         }
      };

      // Install the all-trusting trust manager
      try {
         SSLContext sc = SSLContext.getInstance("SSL");
         sc.init(null, trustAllCerts, new SecureRandom());
         HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
         HttpsURLConnection.setDefaultHostnameVerifier(hv);
      } catch (Exception e) {
      }
   }
}