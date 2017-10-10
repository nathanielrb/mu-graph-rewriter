var button = document.getElementById('rewrite');
var query = document.getElementById('query');
var constraint = document.getElementById('constraint');
var fprops = document.getElementById('fprops');
var result = document.getElementById('result');
var results = document.getElementById('results');



button.onclick = function(){
    var request = new XMLHttpRequest();
    request.onreadystatechange = function(e) {
	if(request.readyState === 4) {
	    results.value = '';
	    if(request.status === 200) { 
		console.log('200');
		var jr = JSON.parse(e.target.responseText);
		result.className = 'filled';
		result.value = jr.rewrittenQuery.trim();
		runButton.disabled = false;
	    } else {
		result.value = 'Error';
		result.className = 'error';
		runButton.disabled = true;
	    } 
	}
    }
    request.open("POST", "/sandbox", true);
    request.send("query=" + query.value + "&constraint=" + constraint.value + "&fprops=" + fprops.value);
};


var runButton = document.getElementById('run');

runButton.onclick = function(){
    var q = result.value;
    if( q == 'Error' )
	return 1;
    else {
	var request = new XMLHttpRequest();
	request.onreadystatechange = function(e) {
	    if(request.readyState === 4) {
		if(request.status === 200) { 
		    console.log('200');
		    var jr = JSON.parse(e.target.responseText);
		    
		    results.className = 'filled';
		    results.value = JSON.stringify(jr.results.bindings, null, 2);
		} else {
		    results.value = 'Error';
		    results.className = 'error';
		} 
	    }
	}
	request.open("POST", "/proxy", true);
	request.send(q);
    }
}
