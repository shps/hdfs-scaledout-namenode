// Setup, just do it
if(typeof(jarmon) === 'undefined') {
    var jarmon = {};
}

var HOST, NDB_COUNT;


jarmon.SET = function (host, ndbcount) {
    HOST = host;
    NDB_COUNT = ndbcount;
}


// This sets up the tabs and the graphs for each tab
jarmon.TAB_RECIPES_STANDARD = function () {
    
    var recipes = [];

    recipes.push(['Memory', ['data_memory', 'index_memory']]);
    recipes.push(['Counters (R/W)', ['counters_sum-simple_reads', 'counters_sum-reads', 'counters_sum-writes']]);
    recipes.push(['Counters (Scans)', ['counters_sum-range_scans', 'counters_sum-table_scans']]);    
    return recipes;
}
 
// The following recipes define the graphs
jarmon.CHART_RECIPES_COLLECTD = function (){
     
    var path = '../jarmon/data/' + HOST; 
    var charts = {};
    
    var i;
    dataList = [];
    for (i=1; i<=NDB_COUNT; i++) {
       dataList.push([path + '/dbi-ndbinfo/gauge-free_data_memory-' + i +'.rrd', 'value', 'node ' + i +' free', '']);
       dataList.push([path + '/dbi-ndbinfo/gauge-total_data_memory-' + i +'.rrd', 'value', 'node ' + i +' total', '']);           
    }
    charts['data_memory'] = {
        title: 'Data Memory',
        data: dataList,
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };
    
    dataList = [];
    for (i=1; i<=NDB_COUNT; i++) {
       dataList.push([path + '/dbi-ndbinfo/gauge-free_index_memory-' + i +'.rrd', 'value', 'node ' + i +' free', '']);
       dataList.push([path + '/dbi-ndbinfo/gauge-total_index_memory-' + i +'.rrd', 'value', 'node ' + i +' total', '']);           
    }
    charts['index_memory'] = {
        title: 'Index Memory',
        data: dataList,
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };

    charts['counters_sum-range_scans'] = {
        title: 'Range Scans',
        data: [
        [path + '/dbi-ndbinfo/gauge-counters_sum-RANGE_SCANS.rrd', 'value', 'range scans', ''],         
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };

    charts['counters_sum-table_scans'] = {
        title: 'Table Scans',
        data: [
        [path + '/dbi-ndbinfo/gauge-counters_sum-TABLE_SCANS.rrd', 'value', 'table scans', ''],         
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };

    charts['counters_sum-simple_reads'] = {
        title: 'Simple Reads',
        data: [
        [path + '/dbi-ndbinfo/gauge-counters_sum-SIMPLE_READS.rrd', 'value', 'simple reads', ''],         
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };

    charts['counters_sum-reads'] = {
        title: 'Reads',
        data: [
        [path + '/dbi-ndbinfo/gauge-counters_sum-READS.rrd', 'value', 'reads', ''],
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };
    
    charts['counters_sum-writes'] = {
        title: 'Writes',
        data: [
        [path + '/dbi-ndbinfo/gauge-counters_sum-WRITES.rrd', 'value', 'writes', ''],         
        ],
        options: jQuery.extend(true, {}, jarmon.Chart.BASE_OPTIONS)
    };    
    
    return charts;

}; // End CHART_RECIPES_COLLECTD
 