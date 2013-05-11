function reduce(key, values) {
		var farr = [];
        var max = -1;
        for (var i = 0; i < values.length; i++) {
        	var arr = values[i].arr;
        	for(var k = 0; k < arr.length;k++) {
        		var c = arr[k].count;
        		farr.push(arr[k]);
        		print("count:" + c);
	        	if(c > max)
	        		max = c;
        	}
        }
      print(farr.length);
      return {arr:farr, max_count:max};
}