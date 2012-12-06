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

    if (JMXPARAM == 'FSNamesystem') {
        recipes.push(['Capacity',   ['capacity']]);
        recipes.push(['Files',   ['files']]);
        recipes.push(['Load',   ['load']]);
        recipes.push(['Heartbeats',   ['heartbeats']]);
        recipes.push(['Replication',   ['replication']]);
        recipes.push(['Blocks',   ['blocks']]);
        recipes.push(['Special Blocks',   ['specialblocksc','specialblockse','specialblocksm','specialblocksp']]);

    } else if (JMXPARAM == 'FSNamesystemState') {
        recipes.push(['Number of DataNodes',   ['numdn']]);        
           
    } else if (JMXPARAM == 'NameNodeActivity') {
        recipes.push(['Read',   ['read']]);        
        recipes.push(['Write',   ['write']]);     
        recipes.push(['Other',   ['other']]);    
        recipes.push(['Average Times',   ['avgtimes']]);    
        recipes.push(['Times',   ['times']]);            
    }
    return recipes;
}
 

// The following recipes define the graphs
jarmon.CHART_RECIPES_COLLECTD = function (){
     
    var path = '../jarmon/data/' + HOST; 
    var charts = {};
 
    if (JMXPARAM == 'FSNamesystem') {
        charts['capacity'] =  {
            title: 'Capacity (GB)',
            data: [
            [path + '/GenericJMX-FSNamesystem/memory-CapacityTotalGB.rrd', 0, 'Total', ''],
            [path + '/GenericJMX-FSNamesystem/memory-CapacityUsedGB.rrd', 0, 'Uses', ''],
            [path + '/GenericJMX-FSNamesystem/memory-CapacityRemainingGB.rrd', 0, 'Remaining', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
    
        charts['files'] =  {
            title: 'Total Files',
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-FilesTotal.rrd', 0, 'Total Files', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
    
        charts['load'] =  {
            title: 'Total Load',
            data: [
            [path + '/GenericJMX-FSNamesystem/gauge-TotalLoad.rrd', 0, 'Total Load', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };    

        charts['heartbeats'] =  {
            title: 'Expired Heartbeats',
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-ExpiredHeartbeats.rrd', 0, 'Expired Heartbeats', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };   
    
        charts['replication'] =  {
            title: 'Block Replication',
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-PendingReplicationBlocks.rrd', 0, 'Pending Replication', ''],
            [path + '/GenericJMX-FSNamesystem/counter-ScheduledReplicationBlocks.rrd', 0, 'Scheduled Replication', ''],
            [path + '/GenericJMX-FSNamesystem/counter-UnderReplicatedBlocks.rrd', 0, 'Under Replicated', ''],        
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };      

        charts['blocks'] =  {
            title: 'Blocks',
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-BlocksTotal.rrd', 0, 'Blocks Total', ''],
            [path + '/GenericJMX-FSNamesystem/counter-BlockCapacity.rrd', 0, 'Block Capacity', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        }; 

        charts['specialblocksc'] =  {
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-CorruptBlocks.rrd', 0, 'Corrupt Blocks', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
        charts['specialblockse'] =  {
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-ExcessBlocks.rrd', 0, 'Excess Blocks', ''],

            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
        charts['specialblocksm'] =  {
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-MissingBlocks.rrd', 0, 'Missing Blocks', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
        charts['specialblocksp'] =  {
            data: [
            [path + '/GenericJMX-FSNamesystem/counter-PendingDeletionBlocks.rrd', 0, 'Pending Deletion Blocks', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };        
    }
 
     else if (JMXPARAM == 'FSNamesystemState') {
        charts['numdn'] =  {
            title: 'Number of DataNodes',
            data: [
            [path + '/GenericJMX-FSNamesystemState/gauge-NumLiveDataNodes.rrd', 0, 'Live', ''],
            [path + '/GenericJMX-FSNamesystemState/gauge-NumDeadDataNodes.rrd', 0, 'Dead', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };
   }
     
    else if (JMXPARAM == 'NameNodeActivity') {
        
        charts['read'] =  {
            title: 'Read Operations',
            data: [
            [path + '/GenericJMX-NameNodeActivity/counter-CreateFileOps.rrd', 0, 'Create File', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-FilesCreated.rrd', 0, 'Files Created', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-FilesAppended.rrd', 0, 'Diles Appended', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-FilesRenamed.rrd', 0, 'Files Renamed', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-DeleteFileOps.rrd', 0, 'Delete File', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-FilesDeleted.rrd', 0, 'Files Deleted', ''],            
            [path + '/GenericJMX-NameNodeActivity/counter-AddBlockOps.rrd', 0, 'Add Block', ''], 
            [path + '/GenericJMX-NameNodeActivity/counter-CreateSymlinkOps.rrd', 0, 'Create Symlink', ''], 
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };     
    
        charts['write'] =  {
            title: 'Write Operations',
            data: [
            [path + '/GenericJMX-NameNodeActivity/counter-GetBlockLocations.rrd', 0, 'GetBlockLocations', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-GetListingOps.rrd', 0, 'GetListing', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-FileInfoOps.rrd', 0, 'FileInfo', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-GetLinkTargetOps.rrd', 0, 'GetLinkTarget', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-FilesInGetListingOps.rrd', 0, 'FilesInGetListing', ''],

            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };     


        charts['other'] =  {
            title: 'Other Operations',
            data: [
            [path + '/GenericJMX-NameNodeActivity/counter-GetAdditionalDatanodeOps.rrd', 0, 'GetAdditionalDatanode', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-BlockReportNumOps.rrd', 0, 'BlockReportNum', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-SyncsNumOps.rrd', 0, 'SyncsNum', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-TransactionsNumOps.rrd', 0, 'TransactionsNum', ''],
            [path + '/GenericJMX-NameNodeActivity/counter-TransactionsBatchedInSync.rrd', 0, 'TransactionsBatchedInSync', ''],

            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };  

        charts['avgtimes'] =  {
            title: 'Average Times',
            data: [
            [path + '/GenericJMX-NameNodeActivity/gauge-TransactionsAvgTime.rrd', 0, 'Transactions Avg Time', ''],
            [path + '/GenericJMX-NameNodeActivity/gauge-SyncsAvgTime.rrd', 0, 'Syncs Avg Time', ''],
            [path + '/GenericJMX-NameNodeActivity/gauge-BlockReportAvgTime.rrd', 0, 'Block Report Avg Time', ''],

            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };  
        
        charts['times'] =  {
            title: 'Times',
            data: [
            [path + '/GenericJMX-NameNodeActivity/gauge-SafeModeTime.rrd', 0, 'SafeMode Time', ''],
            [path + '/GenericJMX-NameNodeActivity/gauge-FsImageLoadTime.rrd', 0, 'Fs Image Load Time', ''],

            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.BASE_OPTIONS)
        };  
    }
    return charts;
};