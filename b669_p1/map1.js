function map() {
	var pagerecords = this.pagerecords;
	if (pagerecords == null)
		return;
	var prs = this.pagerecords;
	for (var key in prs) {
		var content = prs[key];
		var words = content.match(/\w+/g);

		if (words != null) {

			for ( var i = 0; i < words.length; i++) {
				emit({"volumename" : this.volumename,"word" : words[i]}, {"count" : 1});
			}
		}
	}
 }