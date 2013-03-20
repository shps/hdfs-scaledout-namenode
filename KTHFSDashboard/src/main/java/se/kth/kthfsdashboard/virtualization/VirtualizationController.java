/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization;

import com.google.common.base.Optional;
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
import com.google.common.primitives.Ints;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import javax.faces.bean.SessionScoped;
import org.jclouds.compute.domain.internal.TemplateBuilderImpl;
import static org.jclouds.compute.predicates.NodePredicates.TERMINATED;
import static org.jclouds.compute.predicates.NodePredicates.inGroup;
import org.jclouds.ec2.EC2AsyncClient;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.compute.options.EC2TemplateOptions;
import org.jclouds.ec2.domain.InstanceType;
import org.jclouds.ec2.domain.IpProtocol;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.nova.ec2.NovaEC2Client;
import org.jclouds.openstack.nova.v2_0.NovaApi;
import org.jclouds.openstack.nova.v2_0.NovaAsyncApi;
import org.jclouds.openstack.nova.v2_0.compute.NovaComputeService;
import org.jclouds.openstack.nova.v2_0.compute.options.NovaTemplateOptions;
import org.jclouds.openstack.nova.v2_0.domain.Ingress;
import org.jclouds.openstack.nova.v2_0.domain.SecurityGroup;
import org.jclouds.openstack.nova.v2_0.extensions.SecurityGroupApi;
import org.jclouds.openstack.nova.v2_0.features.ServerApi;
import org.jclouds.rest.RestContext;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import static org.jclouds.scriptbuilder.domain.Statements.exec;
import static org.jclouds.scriptbuilder.domain.Statements.extractTargzAndFlattenIntoDirectory;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.kthfsdashboard.user.Group;
import se.kth.kthfsdashboard.virtualization.clusterparser.ClusterController;
import se.kth.kthfsdashboard.virtualization.clusterparser.NodeGroup;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
@ManagedBean
@SessionScoped
public class VirtualizationController implements Serializable {

    private static final URI RUBYGEMS_URI = URI.create("http://production.cf.rubygems.org/rubygems/rubygems-1.8.10.tgz");
    @ManagedProperty(value = "#{computeCredentialsMB}")
    private ComputeCredentialsMB computeCredentialsMB;
    @ManagedProperty(value = "#{clusterController}")
    private ClusterController clusterController;
    private String provider;
    private String id;
    private String key;
    //If Openstack selected, endpoint for keystone API
    private String keystoneEndpoint;
    private ComputeService service;
    private ComputeServiceContext context;

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

    public ClusterController getClusterController() {
        return clusterController;
    }

    public void setClusterController(ClusterController clusterController) {
        this.clusterController = clusterController;
    }

    /*
     * Command to launch the instance
     */
    public void launchCluster() {
        setCredentials();
        service = initContexts();

        createSecurityGroups();
        launchNodesBasicSetup();
        //installRoles();
    }

    /*
     * Private methods used by the controller
     */
    /*
     * Set the credentials chosen by the user to launch the instance
     * retrieves the information from the credentials page
     */
    private void setCredentials() {
        Provider check = Provider.fromString(clusterController.getCluster().getProvider().getName());
        if (!computeCredentialsMB.isAwsec2()
                && Provider.AWS_EC2
                .equals(check)) {
            provider = Provider.AWS_EC2.toString();
            id = computeCredentialsMB.getAwsec2Id();
            key = computeCredentialsMB.getAwsec2Key();
        }

        if (!computeCredentialsMB.isOpenstack()
                && Provider.OPENSTACK
                .equals(check)) {
            provider = Provider.OPENSTACK.toString();
            id = computeCredentialsMB.getOpenstackId();
            key = computeCredentialsMB.getOpenstackKey();
            keystoneEndpoint = computeCredentialsMB.getOpenstackKeystone();
        }

    }
    /*
     * Define the computing cloud service you are going to use
     */

    private ComputeService initContexts() {
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

        context = build.buildView(ComputeServiceContext.class);

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
                template.os64Bit(true);
                template.hardwareId(clusterController.getCluster().getProvider().getInstanceType());
                template.imageId(clusterController.getCluster().getProvider().getImage());
                template.locationId(clusterController.getCluster().getProvider().getRegion());
                break;
            case OPENSTACK:
                template.os64Bit(true);
                template.imageId(clusterController.getCluster().getProvider().getImage());
                template.hardwareId(clusterController.getCluster().getProvider().getRegion()
                        + "/" + clusterController.getCluster().getProvider().getInstanceType());
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
                        .runScript(bootstrap));
                break;
            case OPENSTACK:
                kthfsTemplate.options(NovaTemplateOptions.Builder
                        .overrideLoginUser(clusterController.getCluster().getProvider().getLoginUser())
                        .generateKeyPair(true)
                        .runScript(bootstrap));
                break;
            case RACKSPACE:

                break;
            default:
                throw new AssertionError();
        }

    }

    private void launchNodesBasicSetup() {
        try {
            TemplateBuilder kthfsTemplate = templateKTHFS(provider, service.templateBuilder());
            selectProviderTemplateOptions(provider, kthfsTemplate);
            for (NodeGroup group : clusterController.getCluster().getNodes()) {
                service.createNodesInGroup(group.getSecurityGroup(), group.getNumber(), kthfsTemplate.build());
            }
        } catch (RunNodesException e) {
            System.out.println("error adding nodes to group "
                    + "ups something got wrong on the nodes");
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());

        }
    }

    private void installRoles() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /*
     * Private Method which creates the securitygroups for the cluster 
     * through the rest client implementations in jclouds.
     */
    private void createSecurityGroups() {
        RoleMapPorts commonTCP = new RoleMapPorts(RoleMapPorts.PortType.COMMON);
        RoleMapPorts portsTCP = new RoleMapPorts(RoleMapPorts.PortType.TCP);
        RoleMapPorts portsUDP = new RoleMapPorts(RoleMapPorts.PortType.UDP);

        String region = clusterController.getCluster().getProvider().getRegion();
        //List to gather  ports, we initialize with the ports defined by the user
        List<Integer> globalPorts = new LinkedList<Integer>(clusterController.getCluster().getAuthorizeSpecificPorts());

        //For each basic role, we map the ports in that role into a list which we append to the commonPorts
        for (String commonRole : clusterController.getCluster().getAuthorizePorts()) {
            if (commonTCP.containsKey(commonRole)) {
                List<Integer> portsRole = Ints.asList(commonTCP.get(commonRole));
                globalPorts.addAll(portsRole);
            }
        }


        //If EC2 client
        if (provider.equals(Provider.AWS_EC2.toString())) {
            RestContext<EC2Client, EC2AsyncClient> temp = context.unwrap();
            EC2Client client = temp.getApi();
            //For each group of the security groups
            for (NodeGroup group : clusterController.getCluster().getNodes()) {
                String groupName = "jclouds#" + group.getSecurityGroup();// jclouds way of defining groups
                Set<Integer> openTCP = new HashSet<Integer>(); //To avoid opening duplicate ports
                Set<Integer> openUDP = new HashSet<Integer>();// gives exception upon trying to open duplicate ports in a group
                System.out.printf("%d: creating security group: %s%n", System.currentTimeMillis(),
                        group.getSecurityGroup());
                //create security group
                client.getSecurityGroupServices().createSecurityGroupInRegion(
                        region, groupName, group.getSecurityGroup());
                //get the ports
                for (String authPort : group.getAuthorizePorts()) {

                    //Authorize the ports for TCP and UDP

                    if (portsTCP.containsKey(authPort)) {
                        for (int port : portsTCP.get(authPort)) {
                            if (!openTCP.contains(port)) {
                                client.getSecurityGroupServices().authorizeSecurityGroupIngressInRegion(region,
                                        groupName, IpProtocol.TCP, port, port, "0.0.0.0/0");
                                openTCP.add(port);
                            }
                        }

                        for (int port : portsUDP.get(authPort)) {
                            if (!openUDP.contains(port)) {
                                client.getSecurityGroupServices().authorizeSecurityGroupIngressInRegion(region,
                                        groupName, IpProtocol.UDP, port, port, "0.0.0.0/0");
                                openUDP.add(port);
                            }
                        }
                    }
                }
                //Authorize the global ports TCP
                for (int port : Ints.toArray(globalPorts)) {
                    if (!openTCP.contains(port)) {
                        client.getSecurityGroupServices().authorizeSecurityGroupIngressInRegion(region,
                                groupName, IpProtocol.TCP, port, port, "0.0.0.0/0");
                        openTCP.add(port);
                    }
                }
            }


        }


        //need to test with nova2
        //If openstack nova2 client

        if (provider.equals(Provider.OPENSTACK.toString())) {
            RestContext<NovaApi, NovaAsyncApi> temp = context.unwrap();

            //SecurityGroupApi client = temp.getApi().getSecurityGroupExtensionForZone(region);

            //This stuff below is weird, founded in a code snippet in a workshop on jclouds
            Optional<? extends SecurityGroupApi> securityGroupExt = temp.getApi().getSecurityGroupExtensionForZone(region);
            System.out.println("  Security Group Support: " + securityGroupExt.isPresent());
            if (securityGroupExt.isPresent()) {
                SecurityGroupApi client = securityGroupExt.get();

                //For each group of the security groups
                for (NodeGroup group : clusterController.getCluster().getNodes()) {
                    String groupName = "jclouds#" + group.getSecurityGroup(); //jclouds way of defining groups
                    Set<Integer> openTCP = new HashSet<Integer>(); //To avoid opening duplicate ports
                    Set<Integer> openUDP = new HashSet<Integer>();// gives exception upon trying to open duplicate ports in a group
                    System.out.printf("%d: creating security group: %s%n", System.currentTimeMillis(),
                            group.getSecurityGroup());
                    //create security group
                    SecurityGroup created=client.createWithDescription(groupName, group.getSecurityGroup());
                    //get the ports
                    for (String authPort : group.getAuthorizePorts()) {
                        //Authorize the ports for TCP and UDP
                        if (portsTCP.containsKey(authPort)) {
                            for (int port : portsTCP.get(authPort)) {
                                if (!openTCP.contains(port)) {
                                    Ingress ingress = Ingress.builder()
                                            .fromPort(port)
                                            .toPort(port)
                                            .ipProtocol(org.jclouds.openstack.nova.v2_0.domain.IpProtocol.TCP)
                                            .build();
                                    client.createRuleAllowingSecurityGroupId(created.getId(), ingress, "0.0.0.0/0");
                                    openTCP.add(port);
                                }

                            }
                            for (int port : portsUDP.get(authPort)) {
                                if (!openUDP.contains(port)) {
                                    Ingress ingress = Ingress.builder()
                                            .fromPort(port)
                                            .toPort(port)
                                            .ipProtocol(org.jclouds.openstack.nova.v2_0.domain.IpProtocol.UDP)
                                            .build();
                                    client.createRuleAllowingSecurityGroupId(created.getId(), ingress, "0.0.0.0/0");
                                    openUDP.add(port);
                                }

                            }
                        }

                    }
                    //Authorize the global ports
                    for (int port : Ints.toArray(globalPorts)) {
                        if (!openTCP.contains(port)) {
                            Ingress ingress = Ingress.builder()
                                    .fromPort(port)
                                    .toPort(port)
                                    .ipProtocol(org.jclouds.openstack.nova.v2_0.domain.IpProtocol.TCP)
                                    .build();
                            client.createRuleAllowingSecurityGroupId(created.getId(), ingress, "0.0.0.0/0");
                            openTCP.add(port);
                        }
                    }
                }
            }
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
