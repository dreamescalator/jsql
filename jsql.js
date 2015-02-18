

var dataset = [
  {
    "pageviews": 800.0,
    "evar10": "game_board_topic",
    "visits": 1356756.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 1073179.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271663"
    }
  },
    {
    "pageviews": 500.0,
    "evar10": "game_board_topic",
    "visits": 1356756.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-03-28",
    "uniquevisitors": 1073179.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271663"
    }
  },
      {
    "pageviews": 700.0,
    "evar10": "game_board_topic",
    "visits": 1356756.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 1073179.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271663"
    }
  },
  {
    "pageviews": 300.0,
    "evar10": "game_board_board",
    "visits": 208970.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 114115.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271664"
    }
  },
    {
    "pageviews": 100.0,
    "evar10": "game_board_board",
    "visits": 208970.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 114115.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271664"
    }
  },
    {
    "pageviews": 4000.0,
    "evar10": "game_board_board",
    "visits": 208970.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 114115.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271664"
    }
  },
  {
    "pageviews": 800.0,
    "evar10": "game_answers_question",
    "visits": 419921.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 362980.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271665"
    }
  },
    {
    "pageviews": 555.0,
    "evar10": "game_answers_question",
    "visits": 419921.0,
    "rsid": "cbsicbsiall",
    "evar5": "cbsigamefaqssite",
    "granularity": "day",
    "date": "2015-01-28",
    "uniquevisitors": 362980.0,
    "_id": {
      "$oid": "54dbfb470b43562e3c271665"
    }
  }
]

var und = require('underscore');

/*Helper functions*/
function sum(metric, table){
    var sum = 0;
    var total = {};
    var data = und.pluck(table, metric);
    und.each(data, function(row){ sum += row; });
    var sum_column = "sum_of_"+metric
    total[sum_column] = sum;
    return total;
}

function filter(table, conditions) {return und.where(table, conditions);}

/*
 *Operands:
 *      'lt' = '<'
 *      'gt' = '>'
 *      'eq' = '=='
 *      'neq' = '!='
 *
 */
function date_filter(table, operand, date) {
    var d = new Date(date);
    var f_table;
    var filtered = [];
    var reduced = table.map(function(row){
        var _date =  new Date(row.date);
        if (operand == "gt"){if (_date > d){filtered.push(row);}}
        else if (operand == "lt"){if (_date < d){filtered.push(row);}}
        else if (operand == "eq"){if (_date == d){filtered.push(row);}}
        else if (operand != "neq"){if (_date == d){filtered.push(row);}} 
    });
    return filtered;
}

function parse(conds){
    var date_conds = {};
    var reg_conds = {};
    var operands = ['gt','lt', 'eq', 'neq'];
    und.each(und.keys(conds), function(c){
        if (und.contains(operands, c)) {date_conds[c] = conds[c];}
        else{reg_conds[c] = conds[c];}
    });
    var pair = [];
    pair.push(reg_conds);
    pair.push(date_conds);
    return pair;
}
/*end helper functions */

// Args:
//      cols       = column(s) to retrieve (array of strings);
//      sums       = column(s) to sum (array of strings);
//      conditions = Conditions given as $k:$v pairs (object); this includes date conditions
//      dataset    = an array of objects (collection)
//        
//
//
// 1)   ---> Parse 'conditions', separate 'regular' and 'date' conditions
// 2)       ---> Filter 'table' based on 'regular' conditons. Return Filtered table (f_table)
// 3)          ---> Date_Filter 'f_table' based on 'date' conditons. Return Date_Filtered f_table
// 4)            ---> Run Sum funcs (if any) on date_filtered f_table
// 5)               ---> Make selected_table (s_table) from f_table, append count and sum 
// 6)                  ---> return selected table (s_table)

//Example: select(['evar5','evar10', 'date'], ['pageviews'], {"evar10":"game_board_topic", "lt":"2015-03-01"}, dataset);

function select(cols, sums, conds, table) {
    var reg_conds = parse(conds)[0];
    var date_conds = parse(conds)[1];
    var f_table = filter(table, reg_conds);
    if (und.size(date_conds) > 0) { f_table = date_filter(f_table, und.keys(date_conds), und.values(date_conds));}
    var s_table = [];
    und.map(f_table, function(row){ s_table.push(und.pick(row, cols)); });
    und.map(sums, function(metric){und.extend(s_table[0], sum(metric, f_table), {"total_records":und.size(f_table)});});
    //console.log((s_table)[0]);
    return s_table[0];
}
