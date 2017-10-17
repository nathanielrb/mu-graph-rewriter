var button = document.getElementById('rewrite');
var runButton = document.getElementById('run');
var query = document.getElementById('query');
var constraint = document.getElementById('constraint');
var fprops = document.getElementById('fprops');
var result = document.getElementById('result');
var results = document.getElementById('results');
var rwquery = false;
var annotationsBox = document.getElementById('annotations-box');
var annotations = document.getElementById('annotations');
var queriedAnnotations = document.getElementById('queried-annotations');

function encode(e) {
  return e.replace(/[\<\>\"\^]/g, function(e) {
	return "&#" + e.charCodeAt(0) + ";";
  });
}

// rewrite query
button.onclick = function(){
    var request = new XMLHttpRequest();
    request.onreadystatechange = function(e) {
	if(request.readyState === 4) {
	    results.value = '';
            annotations.innerHTML = '';
            queriedAnnotations.innerHTML = '';
            annotationsBox.style.display = 'none';

	    if(request.status === 200) { 
		console.log('200');
		var jr = JSON.parse(e.target.responseText);
		result.className = 'filled';
                rwquery =  jr.rewrittenQuery.trim();

                var a, an;
                for( var i = 0; i < jr.annotations.length; i++){
                    console.log(jr.annotations[i]);
                    a = jr.annotations[i];
                    an = document.createElement("li");
                    t = a["key"];
                    if( "var" in a ){
                        t += " (" + a["var"] + ")";
                    }
                    an.appendChild(document.createTextNode(t));
                    annotations.appendChild(an);
                }
                for( var i = 0; i < jr.annotations.length; i++){
                    a = jr.queriedAnnotations[i];
                    an = document.createElement("li");
                    t = a["key"];
                    if( "var" in a ){
                        t += ": " + a["var"];
                    }
                    an.appendChild(document.createTextNode(t));
                    queriedAnnotations.appendChild(an);
                }
                annotationsBox.style.display = 'block';

                result.innerHTML = encode(rwquery);
		runButton.disabled = false;
	    } else {
		// result.value = 'Error';
                rwquery = false;
                result.innerHTML = 'Error';
		result.className = 'error';
		runButton.disabled = true;
	    } 
	}
    }
    request.open("POST", "/sandbox", true);
    request.send("query=" + query.value + "&constraint=" + constraint.value + "&fprops=" + fprops.value);
};

// run rewritten query
runButton.onclick = function(){
    if( !rwquery )
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
	request.send(rwquery);
    }
}
