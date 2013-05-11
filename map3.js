function map() {
	var max = this.value.max_count;
	var arr = this.value.arr;
	for(var i = 0; i < arr.length; i++) {
		
		var o = {};
		var a = [];
		
		o.volumename = this._id.volumename;
		o.count = arr[i].count;
		o.max_count = max;
		
		a.push(o);
		
		emit({"word":arr[i].word},{"arr":a,"num_volumes_appeared":1});
	}
}