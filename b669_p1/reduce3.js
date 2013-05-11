function reduce(key, values) {
	var total = 0;
	var ar = [];
	for(var i = 0; i < values.length; i++) {
		total+= values[i].num_volumes_appeared;
		var t = values[i].arr;
		for(var k = 0; k < t.length; k++) {
			ar.push(t[k]);
		}
	}
	return {arr:ar, num_volumes_appeared:total};
}