package se.kth.kthfsdashboard.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import se.kth.kthfsdashboard.host.HostEJB;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class WebCommunication {

   public enum Type {

      STDOUT, STDERR, DO
   }
   private static String NOT_AVAILABLE = "Not available.";
   @EJB
   private HostEJB hostEJB;
   private String hostname;
   private String kthfsInstance;
   private String service;
   private String baseUrl;

   public WebCommunication(String hostname, String kthfsInstance, String service) {
      this.hostname = hostname;
      this.kthfsInstance = kthfsInstance;
      this.service = service;
   }

   public String getStdOut(int n) {
      String url = getBaseUrl(hostname) + "/log/" + kthfsInstance + "/" + service + "/stdout/" + n;
      return getLog(url);
   }

   public String getStdErr(int n) {
      String url = getBaseUrl(hostname) + "/log/" + kthfsInstance + "/" + service + "/stderr/" + n;
      return getLog(url);
   }

   private String getLog(String url) {

      String log = NOT_AVAILABLE;
      try {
         ClientResponse response = getWebResource(url);
         if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
            log = response.getEntity(String.class);
         }
      } catch (Exception ex) {
         Logger.getLogger(WebCommunication.class.getName()).log(Level.SEVERE, null, ex);
      }
      log = log.replaceAll("\n", "<br>");
      return log;
   }

   public String getConfig() {

      String conf = NOT_AVAILABLE;
      String url = baseUrl + "/config/" + kthfsInstance + "/" + service;
      try {
         ClientResponse response = getWebResource(url);
         if (response.getClientResponseStatus().getFamily() == Response.Status.Family.SUCCESSFUL) {
            conf = response.getEntity(String.class);
         }
      } catch (Exception e) {
         Logger.getLogger(WebCommunication.class.getName()).log(Level.SEVERE, null, e);
      }
      return conf;
   }

   public ClientResponse doCommand(String command) throws Exception {

      String url = getBaseUrl(hostname) + "/do/" + kthfsInstance + "/" + service + "/" + command;
//      String url = getBaseUrl(hostname) + "/do/hdfs1/namenode/start";

      return getWebResource(url);
   }

   private String getBaseUrl(String hostname) {

      return "https://" + hostname + ":8090";
   }

   private ClientResponse getWebResource(String url) throws Exception {

      disableCertificateValidation();
      Client client = Client.create();
      WebResource webResource = client.resource(url);
      MultivaluedMap params = new MultivaluedMapImpl();
      params.add("username", "kthfsagent@sics.se");
      params.add("password", "kthfsagent");

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