// Setup, just do it
if(typeof(jarmon) === 'undefined') {
    var jarmon = {};
}

var HOST;


jarmon.SET = function (host) {
    HOST = host;

}


// This sets up the tabs and the graphs for each tab
jarmon.TAB_RECIPES_STANDARD = function () {
    
    var recipes = [];

    recipes.push(['dbi-ndbinfo', ['dbindbinfo']]);
    
    return recipes;
}
 
// The following recipes define the graphs
jarmon.CHART_RECIPES_COLLECTD = function (){
     
    var path = '../jarmon/data/' + HOST; 
    var charts = {};

    charts['dbindbinfo'] = {
        title: 'dbi ndbinfo (free memory)',
        data: [
        [path + '/dbi-ndbinfo/gauge-free_data_memory-1.rrd', 'value', 'data 1', ''],
        [path + '/dbi-ndbinfo/gauge-free_data_memory-2.rrd', 'value', 'data 2', ''],
        [path + '/dbi-ndbinfo/gauge-free_data_memory-3.rrd', 'value', 'data 3', ''],
        [path + '/dbi-ndbinfo/gauge-free_data_memory-4.rrd', 'value', 'data 4', ''],
        [path + '/dbi-ndbinfo/gauge-free_index_memory-1.rrd', 'value', 'index 1', ''],
        [path + '/dbi-ndbinfo/gauge-free_index_memory-2.rrd', 'value', 'index 2', ''],
        [path + '/dbi-ndbinfo/gauge-free_index_memory-3.rrd', 'value', 'index 3', ''],
        [path + '/dbi-ndbinfo/gauge-free_index_memory-4.rrd', 'value', 'index 4', ''],           
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };

    return charts;

}; // End CHART_RECIPES_COLLECTD
 