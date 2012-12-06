// Setup, just do it
if(typeof(jarmon) === 'undefined') {
    var jarmon = {};
}

var HOST, CPU_COUNT, INTERFACES;


jarmon.SET = function (host, cpu_count, interfaces) {
    HOST = host;
    CPU_COUNT = cpu_count;
    INTERFACES = interfaces.split("[")[1].split("]")[0].split(", ");
}


// This sets up the tabs and the graphs for each tab
jarmon.TAB_RECIPES_STANDARD = function () {
    
    var recipes = [];

    var cpus = [];
//    cpus.push('load');
    for (var i=0; i<CPU_COUNT; i++) {
        cpus.push('cpu-' + i);
    }

    var interfacesInfo = [];    
    for (i=0;i<INTERFACES.length ;i++) {
        interfacesInfo.push("if_octets-" + INTERFACES[i]);
    }
//    for (i=0;i<INTERFACES.length ;i++) {
//        interfacesInfo.push("if_packets-" + INTERFACES[i]);
//    }
    for (i=0;i<INTERFACES.length ;i++) {
        interfacesInfo.push("if_errors-" + INTERFACES[i]);
    }
    
    
    recipes.push(['Load',   ['load']]);    
    recipes.push(['CPU',    cpus]);    
    recipes.push(['Memory', ['memory']]);
    recipes.push(['Disk',   ['diskfree']]);
    recipes.push(['Swap',   ['swap']]);    
    recipes.push(['Interface', interfacesInfo]);
    recipes.push(['Users',  ['users']]);    
    
    return recipes;
}
 

// The following recipes define the graphs
jarmon.CHART_RECIPES_COLLECTD = function (){
     
    var path = '../jarmon/data/' + HOST; 
    var n;
    var charts = {};
    
    charts['users'] = {
        title: 'Users',
        data: [
        [path + '/users/users.rrd', 0, 'users', ''],
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
            jarmon.Chart.STACKED_OPTIONS)
    };

    charts['memory'] =  {
        title: 'Physical memory usage',
        data: [
        [path + '/memory/memory-used.rrd', 0, 'Used', ''],
        [path + '/memory/memory-buffered.rrd', 0, 'Buffered', ''],
        [path + '/memory/memory-cached.rrd', 0, 'Cached', ''],            
        [path + '/memory/memory-free.rrd', 0, 'Free', ''],
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
            jarmon.Chart.STACKED_OPTIONS)
    };
    
    charts['swap'] = {
        title: 'Swap',
        data: [
        [path + '/swap/swap-used.rrd', 0, 'Used', ''],
        [path + '/swap/swap-cached.rrd', 0, 'Cached', ''],            
        [path + '/swap/swap-free.rrd', 0, 'Free', ''],
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
            jarmon.Chart.STACKED_OPTIONS)
    };
        
    charts['diskfree'] = {
        title: 'Disk free space',
        data: [
        [path + '/df/df-dev.rrd', 0, 'df-dev', ''],
        [path + '/df/df-root.rrd', 0, 'df-root', ''],
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
            jarmon.Chart.STACKED_OPTIONS)
    };
        
    charts['load'] = {
        title: 'Load Average',
        data: [
        [path + '/load/load.rrd', 'shortterm', 'Short Term', ''],
        [path + '/load/load.rrd', 'midterm', 'Medium Term', ''],
        [path + '/load/load.rrd', 'longterm', 'Long Term', '']
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };
        
    for (n=0; n<CPU_COUNT; n++) {

        charts['cpu-'+n] = {
            title: 'CPU-'+n+' usage',
            data: [
            [path + '/cpu-'+n+'/cpu-wait.rrd', 0, 'Wait', '%'],
            [path + '/cpu-'+n+'/cpu-system.rrd', 0, 'System', '%'],
            [path + '/cpu-'+n+'/cpu-user.rrd', 0, 'User', '%'],
            [path + '/cpu-'+n+'/cpu-interrupt.rrd', 0, 'Interrupt', '%'],
            [path + '/cpu-'+n+'/cpu-nice.rrd', 0, 'Nice', '%'],
            [path + '/cpu-'+n+'/cpu-steal.rrd', 0, 'Steal', '%'],
            [path + '/cpu-'+n+'/cpu-softirq.rrd', 0, 'Soft IRQ', '%'],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS,
                jarmon.Chart.STACKED_OPTIONS)
        };
    }

    for (n=0; n<INTERFACES.length; n++) {
        charts['if_errors-' + INTERFACES[n]] = {
            title: 'Interface Errors ('+INTERFACES[n]+')',
            data: [
            [path + '/interface/if_errors-' + INTERFACES[n] + '.rrd', 'rx', 'RX', ''],
            [path + '/interface/if_errors-' + INTERFACES[n] + '.rrd', 'tx', 'TX', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
        };        
    }

    for (n=0; n<INTERFACES.length; n++) {
        charts['if_octets-' + INTERFACES[n]] = {
            title: 'Interface Traffic ('+INTERFACES[n]+')',
            data: [
            [path + '/interface/if_octets-' + INTERFACES[n] + '.rrd', 'rx', 'RX', ''],
            [path + '/interface/if_octets-' + INTERFACES[n] + '.rrd', 'tx', 'TX', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
        };        
    }

    for (n=0; n<INTERFACES.length; n++) {
        charts['if_packets-' + INTERFACES[n]] = {
            title: 'Interface Packets ('+INTERFACES[n]+')',
            data: [
            [path + '/interface/if_packets-' + INTERFACES[n] + '.rrd', 'rx', 'RX', ''],
            [path + '/interface/if_packets-' + INTERFACES[n] + '.rrd', 'tx', 'TX', ''],
            ],
            options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
        };        
    }

    return charts;

}; // End CHART_RECIPES_COLLECTD
 