var button = document.getElementById('rewrite');
var query = document.getElementById('query');
var constraint = document.getElementById('constraint');
var fprops = document.getElementById('fprops');
var result = document.getElementById('result');

var request = new XMLHttpRequest();

request.onreadystatechange = function(e) {
    if(request.readyState === 4) {
	if(request.status === 200) { 
	    console.log('200');
	    var jr = JSON.parse(e.target.responseText);
	    result.className = 'filled';
	    result.value = jr.rewrittenQuery.trim();
	} else {
	    result.value = 'Error';
	    result.className = 'error';
	} 
    }
}


button.onclick = function(){
    request.open("POST", "/sandbox", true);
    request.send("query=" + query.value + "&constraint=" + constraint.value + "&fprops=" + fprops.value);
};
