/* Teleprompter sync over public MQTT brokers (works across any networks).
   Text is published as a retained message, so a reader that opens later
   gets the current text immediately. Two brokers are used in parallel
   for resilience; messages carry a timestamp so duplicates are ignored. */
(function(){
  var BROKERS=['wss://broker.emqx.io:8084/mqtt','wss://broker.hivemq.com:8884/mqtt'];
  var NS='tplive-7f3a9c/';
  var ALPH='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

  function genCode(){var s='';for(var i=0;i<4;i++)s+=ALPH[Math.floor(Math.random()*ALPH.length)];return s}
  function validCode(c){return typeof c==='string'&&/^[A-Z0-9]{4}$/.test(c)}
  function cid(){return 'tp_'+Math.random().toString(36).slice(2,12)}
  function topics(code){return {t:NS+code+'/t',s:NS+code+'/s'}}

  function connectAll(extra,onMessage,onChange){
    return BROKERS.map(function(url){
      var o={clientId:cid(),clean:true,reconnectPeriod:2000,connectTimeout:10000,keepalive:30};
      if(extra)for(var k in extra)o[k]=extra[k];
      var c=mqtt.connect(url,o);
      c.on('message',function(topic,payload){onMessage(c,topic,payload)});
      ['connect','close','offline','reconnect','error'].forEach(function(ev){c.on(ev,function(){onChange(c,ev)})});
      return c;
    });
  }
  function anyUp(cs){return cs.some(function(c){return c.connected})}

  function reader(code,h){
    var T=topics(code), lastAt=-1, closed=false;
    var cs=connectAll({will:{topic:T.s,payload:'0',retain:true,qos:1}},function(c,topic,payload){
      if(topic!==T.t)return;
      var d;try{d=JSON.parse(payload.toString())}catch(e){return}
      if(!d||typeof d.t!=='string')return;
      var at=+d.at||0; if(at<=lastAt)return; lastAt=at;
      h.onText&&h.onText(d.t);
    },function(c,ev){
      if(ev==='connect'){c.subscribe(T.t,{qos:0});c.publish(T.s,'1',{retain:true,qos:1})}
      if(!closed)h.onState&&h.onState(anyUp(cs));
    });
    function onVis(){if(document.visibilityState==='visible')cs.forEach(function(c){if(c.connected)c.publish(T.s,'1',{retain:true,qos:1})})}
    document.addEventListener('visibilitychange',onVis);
    return {close:function(){closed=true;document.removeEventListener('visibilitychange',onVis);cs.forEach(function(c){try{if(c.connected)c.publish(T.s,'0',{retain:true,qos:1});c.end(false)}catch(e){}})}};
  }

  function writer(code,h){
    var T=topics(code), presence=new Map(), timer=null, pending=null, lastSent=0, closed=false;
    function state(){
      if(closed)return;
      var online=false;presence.forEach(function(v,c){if(c.connected&&v)online=true});
      h.onState&&h.onState({up:anyUp(cs),readerOnline:online,code:code});
    }
    function pub(c,text,at){try{c.publish(T.t,JSON.stringify({t:text,at:at}),{retain:true,qos:0})}catch(e){}}
    var cs=connectAll(null,function(c,topic,payload){
      if(topic===T.s){presence.set(c,payload.toString()==='1');state()}
    },function(c,ev){
      if(ev==='connect'){
        c.subscribe(T.s,{qos:1});
        var init=h.initial?h.initial():'';
        if(init){pub(c,init,lastSent||Date.now())}
      }
      if(ev==='close'||ev==='offline')presence.delete(c);
      state();
    });
    function flush(){timer=null;if(pending===null)return;var t=pending;pending=null;lastSent=Date.now();cs.forEach(function(c){if(c.connected)pub(c,t,lastSent)})}
    return {
      send:function(text){pending=text;if(!timer)timer=setTimeout(flush,80)},
      close:function(){closed=true;clearTimeout(timer);flush();cs.forEach(function(c){try{c.end(false)}catch(e){}})}
    };
  }

  window.TPSync={reader:reader,writer:writer,genCode:genCode,validCode:validCode};
})();
