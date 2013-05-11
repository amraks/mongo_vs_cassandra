function map() {
		
	var w = this._id.word;
	var c = this.value.count;
	var arr = [];
	
	var o = {
			
		word:w,
		count:c
	};
	arr.push(o);
	
	emit({"volumename" : this._id.volumename},{"arr":arr,"max_count":0});
}