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
var resultPanelBox = document.querySelector('.panel-box');
var resultPanel = document.getElementById('result-panel');
var result = document.getElementById('result');
var resultsPanel = document.querySelector('.results');
var results = document.getElementById('results');
var rwquery = false;
var annotationsBox = document.getElementById('annotations-box');
var annotations = document.getElementById('annotations');
var queriedAnnotations = document.getElementById('queried-annotations');
var authorizationInsert = document.getElementById('authorization-insert');
var sessionID = document.getElementById('session-id');
var domainButton = document.getElementById('toggle-domain');
var domain = document.getElementById('domain');

function encode(e) {
  return e.replace(/[\<\>\"\^]/g, function(e) {
	return "&#" + e.charCodeAt(0) + ";";
  });
}

function animate(elem, style, unit, from, to, time, prop) {
    if (!elem) {
        return;
    }
    var start = new Date().getTime(),
        timer = setInterval(function () {
            var step = Math.min(1, (new Date().getTime() - start) / time);
            if (prop) {
                elem[style] = (from + step * (to - from))+unit;
            } else {
                elem.style[style] = (from + step * (to - from))+unit;
            }
            if (step === 1) {
                clearInterval(timer);
            }
        }, 25);
    if (prop) {
          elem[style] = from+unit;
    } else {
          elem.style[style] = from+unit;
    }
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
        writeConstraint.style.background = '#fff';
        writeConstraint.style.height = '400px';
    }
}

domainButton.onclick = function() {
    domain.style.display = domain.style.display == 'block' ? 'none' : 'block';
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
		resultPanelBox.className = 'panel-box filled';
                rwquery =  jr.rewrittenQuery.trim();

                var a, an;
                if( jr.annotations.length > 0 || jr.queriedAnnotations.length > 0 ){
                    for( var i = 0; i < jr.annotations.length; i++){
                        console.log(jr.annotations[i]);
                        a = jr.annotations[i];
                        an = document.createElement("li");
                        t = a["key"];
                        if( "var" in a ){
                            t += ": " + a["var"];
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
                    //animate(document.body, "scrollTop", "", 0, resultPanel.offsetTop, 500, true);
                    location.hash = '#result-panel';

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
                    // animate(document.body, "scrollTop", "", 0, resultsPanel.offsetTop, 1000, true);
                    location.hash = '#results-panel';
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
