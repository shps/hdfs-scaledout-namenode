// Setup, just do it
if(typeof(jarmon) === 'undefined') {
    var jarmon = {};
}


var HOST;
var JMXPARAM;


jarmon.SET = function (host, jmx_param) {
    HOST = host;
    JMXPARAM = jmx_param;
}

// This sets up the tabs and the graphs for each tab
jarmon.TAB_RECIPES_STANDARD = function () {
    
    var recipes = [];

    if (JMXPARAM == 'DataNodeActivity') {

        recipes.push(['Heartbeats',   ['hbnum','hbavg']]);
        recipes.push(['Bytes', ['bytes']]);
        recipes.push(['Client Operations', ['clientops']]);
        recipes.push(['Blocks', ['blocks']]);
        recipes.push(['Block Operations', ['blockops','blockopsavgtime' , 'blockops2', 'blockops2avgtime']]);          
        
        recipes.push(['Failures', ['bvfailure','vfailure']]);
        
    }
//    else if (JMXPARAM == 'FSNamesystemState') {
//        recipes.push(['Number of DataNodes',   ['numdn']]);        
//           
//    } 
    return recipes;
}
 
 
 
// The following recipes define the graphs
jarmon.CHART_RECIPES_COLLECTD = function (){
     
    var path = '../jarmon/data/' + HOST; 
    var charts = {};
 
    if (JMXPARAM == 'DataNodeActivity') {
    
        charts['hbnum'] =  {
            title: 'Number of Heartbeats',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-HeartbeatsNumOps.rrd', 0, 'Number of Heartbeats', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
        charts['hbavg'] =  {
            title: 'Heartbeats Average Time',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-HeartbeatsAvgTime.rrd', 0, 'Heartbeats Average Time', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };        
          
        charts['bytes'] =  {
            title: 'Bytes Read/Written',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-BytesRead.rrd', 0, 'Bytes Read', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-BytesWritten.rrd', 0, 'Bytes Written', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };

        charts['clientops'] =  {
            title: 'Client Operations',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-ReadsFromLocalClient.rrd', 0, 'Reads: Local Client', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-ReadsFromRemoteClient.rrd', 0, 'Reads: Remote Client', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-WritesFromLocalClient.rrd', 0, 'Writes: Remote Client', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-WritesFromRemoteClient.rrd', 0, 'Writes: Remote Client', ''],            
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
        
        charts['blocks'] =  {
            title: 'Blocks',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-BlocksRead.rrd', 0, 'Blocks Read', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-BlocksWritten.rrd', 0, 'Blocks Written', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-BlocksRemoved.rrd', 0, 'Blocks Removed', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-BlocksReplicated.rrd', 0, 'Blocks Replicated', ''],            
            [path + '/GenericJMX-DataNodeActivity/gauge-BlocksVerified.rrd', 0, 'Blocks Verified', ''], 
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };

        charts['blockops'] =  {
            title: 'Number of Block Operations',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-ReadBlockOpNumOps.rrd', 0, 'Read Block', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-WriteBlockOpNumOps.rrd', 0, 'Write Block', ''],
            [path + '/GenericJMX-DataNodeActivity/counter-CopyBlockOpNumOps.rrd', 0, 'Copy Block', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-ReplaceBlockOpNumOps.rrd', 0, 'Replace Block', ''],            
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };  

        charts['blockopsavgtime'] =  {
            title: 'Block Operations Average Time',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-ReadBlockOpAvgTime.rrd', 0, 'Read Block', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-WriteBlockOpAvgTime.rrd', 0, 'Write Block', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-CopyBlockOpAvgTime.rrd', 0, 'Copy Block', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-ReplaceBlockOpAvgTime.rrd', 0, 'Replace Block', ''],            
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        }; 

        charts['blockops2'] =  {
            title: 'Number of Block Operations',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-BlockChecksumOpNumOps.rrd', 0, 'Block Checksum', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-BlockReportsNumOps.rrd', 0, 'Block Reports', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        }; 

        charts['blockops2avgtime'] =  {
            title: 'Block Operations Average Time',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-BlockChecksumOpAvgTime.rrd', 0, 'Block Checksum', ''],
            [path + '/GenericJMX-DataNodeActivity/gauge-BlockReportsAvgTime.rrd', 0, 'Block Reports', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        }; 
        
        charts['bvfailure'] =  {
            title: 'Block Verification Failures',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-BlockVerificationFailures.rrd', 0, 'Block Verification Failures', ''],           
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        }; 

        charts['vfailure'] =  {
            title: 'Volume Failures',
            data: [
            [path + '/GenericJMX-DataNodeActivity/gauge-VolumeFailures.rrd', 0, 'Volume Failures', ''],           
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };

    }





//     else if (JMXPARAM == 'FSNamesystemState') {
//        charts['numdn'] =  {
//            title: 'Number of DataNodes',
//            data: [
//            [path + '/GenericJMX-FSNamesystemState/gauge-NumLiveDataNodes.rrd', 0, 'Live', ''],
//            [path + '/GenericJMX-FSNamesystemState/gauge-NumDeadDataNodes.rrd', 0, 'Dead', ''],
//            ],
//            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
//                jarmon.Chart.BASE_OPTIONS)
//        };
//   }
 
    return charts;
};