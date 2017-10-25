var button = document.getElementById('rewrite');
var runButton = document.getElementById('run');
var modelButton = document.getElementById('model');
var modelMessage = document.getElementById('model-message');
var viewModel =  document.getElementById('preview');
var query = document.getElementById('query');
var readConstraint = document.getElementById('read-constraint');
var writeConstraint = document.getElementById('write-constraint');
var readwrite = document.getElementById('read-write');
var fprops = document.getElementById('fprops');
var resultPanel = document.querySelector('.panel-box');
var result = document.getElementById('result');
var resultsPanel = document.querySelector('.results');
var results = document.getElementById('results');
var rwquery = false;
var annotationsBox = document.getElementById('annotations-box');
var annotations = document.getElementById('annotations');
var queriedAnnotations = document.getElementById('queried-annotations');
var authorizationInsert = document.getElementById('authorization-insert');
var sessionID = document.getElementById('session-id');

function encode(e) {
  return e.replace(/[\<\>\"\^]/g, function(e) {
	return "&#" + e.charCodeAt(0) + ";";
  });
}

readwrite.onchange = function(e){ 
    if(e.target.checked){
        writeConstraint.value = '';
        writeConstraint.disabled = true;
        writeConstraint.style.background = '#ddd';
        writeConstraint.style.height = '40px';
    }
    else { 
        writeConstraint.value = readConstraint.value;
        writeConstraint.disabled = false;
        writeConstraint.style.background = '#eee';
        writeConstraint.style.height = '400px';
    }
}

// rewrite query
button.onclick = function(){
    var request = new XMLHttpRequest();
    request.onreadystatechange = function(e) {
	if(request.readyState === 4) {
            result.className = '';
	    results.value = '';
            resultsPanel.style.display = "none";
            annotations.innerHTML = '';
            queriedAnnotations.innerHTML = '';
            annotationsBox.style.display = 'none';

	    if(request.status === 200) { 
		console.log('200');
		var jr = JSON.parse(e.target.responseText);
		resultPanel.className = 'panel-box filled';
                rwquery =  jr.rewrittenQuery.trim();

                var a, an;
                if( jr.annotations.length > 0 || jr.queriedAnnotations.length > 0 ){
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
                    for( var i = 0; i < jr.queriedAnnotations.length; i++){
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
                }

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
    request.send("query=" + escape(query.value)
                 + "&readconstraint=" + escape(readConstraint.value)
                 + "&writeconstraint=" + escape((readwrite.checked ? readConstraint.value : writeConstraint.value))
                 + "&fprops=" + escape(fprops.value)
                 + "&session-id=" + escape(sessionID.value)
                + "&authorization-insert=" + escape(authorizationInsert.value));
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
                    resultsPanel.style.display = "block";
		} else {
		    results.value = 'Error';
		    results.className = 'error';
		} 
	    }
	}
	request.open("POST", "/proxy", true);
	request.send(rwquery);
    }
};

modelButton.onclick = function(){
    var request = new XMLHttpRequest();
    request.onreadystatechange = function(e) {
	if(request.readyState === 4) {
            result.className = '';
	    results.value = '';
            resultsPanel.style.display = "none";
            annotations.innerHTML = '';
            queriedAnnotations.innerHTML = '';
            annotationsBox.style.display = 'none';

	    if(request.status === 200) { 
		console.log('200');
                jr = JSON.parse(e.target.responseText);
                modelMessage.style.display = "inline";
                modelMessage.innerHTML = 'Model applied.';
                preview.style.display="inline";
	    } else {
                preview.style.display="none";
                modelMessage.innerHTML = 'Error applying model.';
	    } 
	}
    }
    request.open("POST", "/model", true);
    request.send("&readconstraint=" + readConstraint.value
                 + "&writeconstraint=" + (readwrite.checked ? readConstraint.value : writeConstraint.value)
                 + "&fprops=" + fprops.value
                 + "&session-id=" + sessionID.value
                + "&authorization-insert=" + authorizationInsert.value);
};
