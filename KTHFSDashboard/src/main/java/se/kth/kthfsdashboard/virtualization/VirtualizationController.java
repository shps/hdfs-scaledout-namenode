/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization;

import com.google.common.base.Predicates;
import static com.google.common.base.Predicates.not;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Module;
import java.io.Serializable;
import java.net.URI;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import static org.jclouds.Constants.PROPERTY_CONNECTION_TIMEOUT;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.domain.Region;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_AMI_QUERY;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_CC_AMI_QUERY;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_CC_REGIONS;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_PORT_OPEN;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_SCRIPT_COMPLETE;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.events.StatementOnNodeCompletion;
import org.jclouds.compute.events.StatementOnNodeFailure;
import org.jclouds.compute.events.StatementOnNodeSubmission;
import org.jclouds.compute.options.RunScriptOptions;
import static com.google.common.io.Closeables.closeQuietly;
import static org.jclouds.compute.predicates.NodePredicates.TERMINATED;
import static org.jclouds.compute.predicates.NodePredicates.inGroup;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.ec2.domain.InstanceType;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.nova.v2_0.compute.options.NovaTemplateOptions;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import static org.jclouds.scriptbuilder.domain.Statements.exec;
import static org.jclouds.scriptbuilder.domain.Statements.extractTargzAndFlattenIntoDirectory;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.kthfsdashboard.virtualization.clusterparser.ClusterController;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
@ManagedBean
@RequestScoped
public class VirtualizationController implements Serializable {

    private static final URI RUBYGEMS_URI = URI.create("http://production.cf.rubygems.org/rubygems/rubygems-1.8.10.tgz");
    @ManagedProperty(value = "#{messageController}")
    private MessageController messages;
    @ManagedProperty(value = "#{computeCredentialsMB}")
    private ComputeCredentialsMB computeCredentialsMB;
    @ManagedProperty(value= "#{clusterController}")
    private ClusterController clusterController;
    private String provider;
    private String id;
    private String key;
    //If Openstack selected, endpoint for keystone API
    private String keystoneEndpoint;

    /**
     * Creates a new instance of VirtualizationController
     */
    public VirtualizationController() {
    }

    public ComputeCredentialsMB getComputeCredentialsMB() {
        return computeCredentialsMB;
    }

    public void setComputeCredentialsMB(ComputeCredentialsMB computeCredentialsMB) {
        this.computeCredentialsMB = computeCredentialsMB;
    }

    public MessageController getMessages() {
        return messages;
    }

    public void setMessages(MessageController messages) {
        this.messages = messages;
    }


    /*
     * Command to launch the instance
     */
    public void launchInstance() {
        messages.addMessage("Configuring service context for " + provider);
        //setCredentials();
        ComputeService service = initComputeService();

        try {

            TemplateBuilder kthfsTemplate = templateKTHFS(provider, service.templateBuilder());

            messages.addMessage("Configuring bootstrap script and specifying ports for security group");

            selectProviderTemplateOptions(provider, kthfsTemplate);

            messages.addMessage("Security Group Ports [22, 80, 8080, 8181, 8686, 8983, 4848, 4040, 4000, 443]");
            //changes
            //NodeMetadata node = getOnlyElement(service.createNodesInGroup(providerMB.getGroupName(), 1, kthfsTemplate.build()));
//
//
//            String address = "";
//
//            for (String ip : node.getPublicAddresses()) {
//                address = ip;
//                break;
//            }
//
//            messages.addSuccessMessage("http://" + address + ":8080/KTHFSDashboard");
//            messages.addMessage("VM launched with private IP: " + node.getPrivateAddresses() + ", public IP: " + node.getPublicAddresses());
//            messages.addMessage("Running chef-solo, deploying Architecture");
//            service.runScriptOnNodesMatching(
//                    Predicates.<NodeMetadata>and(not(TERMINATED), inGroup(providerMB.getGroupName())), runChefSolo(),
//                    RunScriptOptions.Builder.nameTask("runchef-solo")
//                    .overrideLoginCredentials(node.getCredentials()));


//            for (Map.Entry<? extends NodeMetadata, ExecResponse> response : responses.entrySet()) {
//                System.out.printf("<< node %s: %s%n", response.getKey().getId(),
//                        concat(response.getKey().getPrivateAddresses(), response.getKey().getPublicAddresses()));
//                System.out.printf("<<     %s%n", response.getValue());
//            }
//
//            System.out.printf("<< node %s: %s%n", node.getId(),
//                    concat(node.getPrivateAddresses(), node.getPublicAddresses()));

//        } catch (RunNodesException e) {
//            messages.addErrorMessage("error adding node to group " + providerMB.getGroupName()
//                    + "ups something got wrong on the node");
        } catch (Exception e) {
            //System.err.println("error: " + e.getMessage());
            messages.addErrorMessage("Error: " + e.getMessage());
        } finally {
            closeQuietly(service.getContext());
        }
    }

   
    //NEED TO CHANGE FOR USING PARSING FILE
    /*
     * Private methods used by the controller
     */
    /*
     * Set the credentials chosen by the user to launch the instance
     * retrieves the information from the credentials page
     */
//    private void setCredentials() {
//        if (providerMB.getProviderName().equals("Amazon-EC2")) {
//            provider = Provider.AWS_EC2.toString();
//            id = computeCredentialsMB.getAwsec2Id();
//            key = computeCredentialsMB.getAwsec2Key();
//        }
//        if (providerMB.getProviderName().equals("OpenStack")) {
//            provider = Provider.OPENSTACK.toString();
//            id = computeCredentialsMB.getOpenstackId();
//            key = computeCredentialsMB.getOpenstackKey();
//            keystoneEndpoint = computeCredentialsMB.getOpenstackKeystone();
//        }
//        if (providerMB.getProviderName().equals("Rackspace")) {
//            provider = Provider.RACKSPACE.toString();
//            id = computeCredentialsMB.getRackspaceId();
//            key = computeCredentialsMB.getRackspaceKey();
//        }
//    }
    /*
     * Define the computing cloud service you are going to use
     */

    private ComputeService initComputeService() {
        Provider check = Provider.fromString(provider);
        //We define the properties of our service
        Properties serviceDetails = serviceProperties(check);

        // example of injecting a ssh implementation
        // injecting the logging module
        Iterable<Module> modules = ImmutableSet.<Module>of(
                new SshjSshClientModule(),
                new SLF4JLoggingModule(),
                new EnterpriseConfigurationModule());

        ContextBuilder build = null;
        switch (check) {
            case AWS_EC2:
                build = ContextBuilder.newBuilder(provider)
                        .credentials(id, key)
                        .modules(modules)
                        .overrides(serviceDetails);
                break;
            case OPENSTACK:
                build = ContextBuilder.newBuilder(provider)
                        .endpoint(keystoneEndpoint)
                        .credentials(id, key)
                        .modules(modules)
                        .overrides(serviceDetails);
                break;
            case RACKSPACE:
                build = ContextBuilder.newBuilder(provider)
                        .credentials(id, key)
                        .modules(modules)
                        .overrides(serviceDetails);
                break;
        }

        if (build == null) {
            throw new NullPointerException("Not selected supported provider");
        }

        ComputeServiceContext context = build.buildView(ComputeServiceContext.class);

        //From minecraft example, how to include your own event handlers!
        context.utils().eventBus().register(ScriptLogger.INSTANCE);

        return context.getComputeService();

    }

    /*
     * Define the service properties for the compute service context using
     * Amazon EC2 like Query parameters and regions. Does the same for Openstack and Rackspace
     * 
     * Includes time using the ports when launching the VM instance executing the script
     */
    private Properties serviceProperties(Provider provider) {
        Properties properties = new Properties();
        long scriptTimeout = TimeUnit.MILLISECONDS.convert(50, TimeUnit.MINUTES);
        properties.setProperty(TIMEOUT_SCRIPT_COMPLETE, scriptTimeout + "");
        properties.setProperty(TIMEOUT_PORT_OPEN, scriptTimeout + "");
        properties.setProperty(PROPERTY_CONNECTION_TIMEOUT, scriptTimeout + "");

        switch (provider) {
            case AWS_EC2:
                properties.setProperty(PROPERTY_EC2_AMI_QUERY, "owner-id=137112412989;state=available;image-type=machine");
                properties.setProperty(PROPERTY_EC2_CC_AMI_QUERY, "");
                properties.setProperty(PROPERTY_EC2_CC_REGIONS, Region.EU_WEST_1);
                break;
            case OPENSTACK:
                break;
            case RACKSPACE:
                break;
        }

        return properties;
    }


    /*
     * Template of the VM we want to launch using EC2, or Openstack
     */
    private TemplateBuilder templateKTHFS(String provider, TemplateBuilder template) {
        Provider check = Provider.fromString(provider);
        switch (check) {
            case AWS_EC2:
                template.osFamily(OsFamily.UBUNTU);
                template.os64Bit(true);
                template.hardwareId(InstanceType.M1_LARGE);
                template.imageId(Region.EU_WEST_1 + "/ami-ffcdce8b");
                template.locationId(Region.EU_WEST_1);
                break;
            case OPENSTACK:
                template.os64Bit(true);
                template.imageNameMatches("Ubuntu_12.04");
                template.hardwareId("RegionSICS/" + 4);
                break;
            case RACKSPACE:
                break;
            default:
                throw new AssertionError();

        }


        return template;
    }


    /*
     * Bootscrap Script for the nodes to launch the KTHFS Dashboard and install Chef Solo
     */
    private StatementList initBootstrapScript() {

        ImmutableList.Builder<Statement> bootstrapBuilder = ImmutableList.builder();
        bootstrapBuilder.add(exec("apt-get update;"));
        bootstrapBuilder.add(exec("apt-get install -y ruby1.9.1-full;"));
        bootstrapBuilder.add(new StatementList(//
                exec("if ! hash gem 2>/dev/null; then"), //
                exec("("), //
                extractTargzAndFlattenIntoDirectory(RUBYGEMS_URI, "/tmp/rubygems"), //
                exec("{cd} /tmp/rubygems"), //
                exec("ruby setup.rb --no-format-executable"), //
                exec("{rm} -fr /tmp/rubygems"), //
                exec(")"), //
                exec("fi"), //
                exec("gem install chef -v 10.20.0 --no-rdoc --no-ri")));
        bootstrapBuilder.add(exec("sudo mkdir /etc/chef;"));
        bootstrapBuilder.add(exec("cd /etc/chef;"));
        bootstrapBuilder.add(exec("sudo wget http://lucan.sics.se/kthfs/solo.rb;"));


        return new StatementList(bootstrapBuilder.build());
    }

    /*
     * Script to run Chef Solo
     */
    private StatementList runChefSolo() {
        ImmutableList.Builder<Statement> bootstrapBuilder = ImmutableList.builder();
        bootstrapBuilder.add(exec("sudo chef-solo -c /etc/chef/solo.rb -j http://lucan.sics.se/kthfs/chef.json -r http://lucan.sics.se/kthfs/kthfs-dash.tar.gz;"));
        return new StatementList(bootstrapBuilder.build());
    }

    /*
     * Select extra options depending of the provider we selected
     */
    private void selectProviderTemplateOptions(String provider, TemplateBuilder kthfsTemplate) {
        Provider check = Provider.fromString(provider);
        StatementList bootstrap = initBootstrapScript();
        switch (check) {
            case AWS_EC2:
                kthfsTemplate.options(EC2TemplateOptions.Builder
                        .inboundPorts(22, 80, 8080, 8181, 8686, 8983, 4848, 4040, 4000, 443)
                        
                        .runScript(bootstrap));
                break;
            case OPENSTACK:
                kthfsTemplate.options(NovaTemplateOptions.Builder
                        .inboundPorts(22, 80, 8080, 8181, 8686, 8983, 4848, 4040, 4000, 443)
                        .overrideLoginUser("ubuntu")
                        .generateKeyPair(true)
                        
                        .runScript(bootstrap));
                break;
            case RACKSPACE:

                break;
            default:
                throw new AssertionError();
        }

    }

    static enum ScriptLogger {

        INSTANCE;
        Logger logger = LoggerFactory.getLogger(VirtualizationController.class);

        @Subscribe
        @AllowConcurrentEvents
        public void onStart(StatementOnNodeSubmission event) {
            logger.info(">> running {} on node({})", event.getStatement(), event.getNode().getId());
            if (logger.isDebugEnabled()) {
                logger.debug(">> script for {} on node({})\n{}", new Object[]{event.getStatement(), event.getNode().getId(),
                            event.getStatement().render(org.jclouds.scriptbuilder.domain.OsFamily.UNIX)});
            }
        }

        @Subscribe
        @AllowConcurrentEvents
        public void onFailure(StatementOnNodeFailure event) {
            logger.error("<< error running {} on node({}): {}", new Object[]{event.getStatement(), event.getNode().getId(),
                        event.getCause().getMessage()}, event.getCause());
        }

        @Subscribe
        @AllowConcurrentEvents
        public void onSuccess(StatementOnNodeCompletion event) {
            ExecResponse arg0 = event.getResponse();
            if (arg0.getExitStatus() != 0) {
                logger.error("<< error running {} on node({}): {}", new Object[]{event.getStatement(), event.getNode().getId(),
                            arg0});
            } else {
                logger.info("<< success executing {} on node({}): {}", new Object[]{event.getStatement(),
                            event.getNode().getId(), arg0});
            }
        }
    }
}
