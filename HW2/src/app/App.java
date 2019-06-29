package app;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.NetworkTopology;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class App {
    static int nextBrokerNumber = 0;
    static int nextDatacenterNumber = 0;
    static int nextVMId = 0;
    static int nextCloudletId = 0;
    static int nextHostId = 0;
    
    public static void main(String[] args) throws Exception {
        CloudSim.init(3, Calendar.getInstance(), false);
        
        Datacenter dc0 = createDatacenter();
        Datacenter dc1 = createDatacenter();
        Datacenter dc2 = createDatacenter();
        
        DatacenterBroker broker1 = createBroker();
        DatacenterBroker broker2 = createBroker();
        DatacenterBroker broker3 = createBroker();

        HashMap<String, String> vmOptions = new HashMap<String, String>() {{ put("mips", "250"); }}; 
        Vm vm1 = createVM(broker1.getId(), vmOptions, new CloudletSchedulerTimeShared());
        Vm vm2 = createVM(broker2.getId(), vmOptions, new CloudletSchedulerTimeShared());
        Vm vm3 = createVM(broker3.getId(), vmOptions, new CloudletSchedulerTimeShared());
        
        ArrayList<Cloudlet> cloudlet1 = new ArrayList<Cloudlet>(1) {{ add(createCloudlet(broker1.getId(), vm1.getId(), new UtilizationModelFull())); }};
        ArrayList<Cloudlet> cloudlet2 = new ArrayList<Cloudlet>(1) {{ add(createCloudlet(broker2.getId(), vm2.getId(), new UtilizationModelFull())); }};
        ArrayList<Cloudlet> cloudlet3 = new ArrayList<Cloudlet>(1) {{ add(createCloudlet(broker3.getId(), vm3.getId(), new UtilizationModelFull())); }};

        broker1.submitVmList(new ArrayList<Vm>() {{ add(vm1); }} );
        broker1.submitCloudletList(cloudlet1);

        broker2.submitVmList(new ArrayList<Vm>() {{ add(vm2); }} );
        broker2.submitCloudletList(cloudlet2);

        broker3.submitVmList(new ArrayList<Vm>() {{ add(vm3); }} );
        broker3.submitCloudletList(cloudlet3);

        // setup network
        System.out.println("PWD: " + System.getProperty("user.dir"));
        NetworkTopology.buildNetworkTopology("HW2/topology.brite");

        // datacenter network mappings
        NetworkTopology.mapNode(dc0.getId(), 0);
        NetworkTopology.mapNode(dc1.getId(), 2);
        NetworkTopology.mapNode(dc2.getId(), 7);

        // broker network mappings
        NetworkTopology.mapNode(broker1.getId(), 3);
        NetworkTopology.mapNode(broker2.getId(), 5);
        NetworkTopology.mapNode(broker3.getId(), 9);

        CloudSim.startSimulation();
        List<Cloudlet> output1 = broker1.getCloudletReceivedList();
        List<Cloudlet> output2 = broker2.getCloudletReceivedList();
        List<Cloudlet> output3 = broker3.getCloudletReceivedList();
        CloudSim.stopSimulation();
        
        printCloudletList(output1);
        printCloudletList(output2);
        printCloudletList(output3);
    }
    
    private static Vm createVM(int brokerId, Map<String, String> options, CloudletScheduler scheduler) {
        double mips = 1000;
        int ram = 512;
        
        if (options.containsKey("mips")) {
            mips = Double.parseDouble(options.get("mips"));
        }

        if (options.containsKey("ram")) {
            ram = Integer.parseInt(options.get("ram"));
        }
        
        return new Vm(
            nextVMId++,
            brokerId,
            mips,
            1,
            ram,
            1000,
            10000,
            "Xen",
            new CloudletSchedulerTimeShared()
        );
    }
    
    private static Cloudlet createCloudlet(int brokerId, int vmId, UtilizationModel model) {
        Cloudlet retVal = new Cloudlet(
                                nextCloudletId++,
                                40000,
                                1,
                                300,
                                300,
                                model,
                                model,
                                model
                            );
        
        retVal.setUserId(brokerId);
        
        if (vmId != -1) {
            retVal.setVmId(vmId);
        }
        
        return retVal;
    }
    
    private static DatacenterBroker createBroker() {
        DatacenterBroker broker = null;
        
        try {
            broker = new DatacenterBroker("Broker"+(nextBrokerNumber++));
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return broker;
    }
    
    private static Datacenter createDatacenter() {
        Datacenter dc = null;
        
        Pe pe = new Pe(0, new PeProvisionerSimple(1000));
        List peList = Arrays.asList(pe);
        
        Host host = new Host(
                nextHostId++,
                new RamProvisionerSimple(2 * 1024),
                new BwProvisionerSimple(10000),
                1000000,
                peList,
                new VmSchedulerSpaceShared(peList)
        );
        List hostList = Arrays.asList(host);
        
        DatacenterCharacteristics chars = new DatacenterCharacteristics(
                "x86",
                "Linux",
                "Xen",
                Arrays.asList(host),
                6,
                3,
                0.05,
                0.001,
                0.0
        );
        
        try {
            dc = new Datacenter("Datacenter"+(nextDatacenterNumber++), chars, new VmAllocationPolicySimple(hostList), Arrays.asList(), 0);
        } catch (Exception ex) {
            Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return dc;
    }
    
    private static void printCloudletList(List<Cloudlet> list) {
        int size = list.size();
        Cloudlet cloudlet;

        String format = "%15s\t%15s\t%15s\t%10s\t%10s\t%15s\t%15s\n";
        // String indent = "    ";
        System.out.println();
        System.out.println("========== OUTPUT ==========");
        System.out.printf(format, "Cloudlet ID", "STATUS", "Data center ID", "VM ID", "Time", "Start Time", "Finish Time");
        // Log.printLine();
        // Log.printLine("========== OUTPUT ==========");
        // Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
        //                 + "Data center ID" + indent + "VM ID" + indent + "Time" + indent
        //                 + "Start Time" + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
            cloudlet = list.get(i);
            // Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            System.out.printf(format, cloudlet.getCloudletId(),
                                      (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS ? "SUCCESS": "FAILED"),
                                      cloudlet.getResourceName(cloudlet.getResourceId()), 
                                      cloudlet.getVmId(), 
                                      dft.format(cloudlet.getActualCPUTime()),
                                      dft.format(cloudlet.getExecStartTime()),
                                      dft.format(cloudlet.getFinishTime()));

            // if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
            //         Log.print("SUCCESS");

            //         Log.printLine(indent + indent + cloudlet.getResourceName(cloudlet.getResourceId())
            //                         + indent + indent + indent + cloudlet.getVmId()
            //                         + indent + indent
            //                         + dft.format(cloudlet.getActualCPUTime()) + indent
            //                         + indent + dft.format(cloudlet.getExecStartTime())
            //                         + indent + indent
            //                         + dft.format(cloudlet.getFinishTime()));
            // }
        }
    }    
}