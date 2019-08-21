<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>Ad visibility test</title>
</head>
<body>
<?php
$id1=$_POST["id1"];
$id2=$_POST["id2"];
$id3=$_POST["id3"];
$id4=$_POST["id4"];
$id5=$_POST["id5"];

// $tag= "<script type=\"text/javascript\">
//  var NLPTagOptions = {
//   key: 'faed4ea23e1a52fa3fb1651cc710e20c',
//   md1:2,
//   td1:'nlpCaptchaOuterContainer',
//   source: '3rdParty',
//   ct:'still',
//   id1:'$id1',
//   id2:'$id2',
//   id3:'$id3',
//   id4:'$id4',
//   id5:'$id5'
//  };
// </script>
// <script type=\"text/javascript\" src=\"https://call.nlpcaptcha.in/js/simpli5d.js\"></script>"

// echo console.log($tag);

?>
<div style="height:1000px;">Scroll down to view image</div>
 
  <div id="nlpCaptchaOuterContainer" style="margin-bottom:300px;height: 184px;width: 312px">
	<img src="http://img.clipartfest.com/b914c71b0844048325e35030698bb8b2_cool-beans-coolbeanseatery-cool-beans-clipart_1362-1360.png" height="184px" width="312px" />
<?php
	echo "<script type=\"text/javascript\">
 var NLPTagOptions = {
  key: 'faed4ea23e1a52fa3fb1651cc710e20c',
  md1:2,
  td1:'nlpCaptchaOuterContainer',
  source: '3rdParty',
  ct:'still',
  id1:'$id1',
  id2:'$id2',
  id3:'$id3',
  id4:'$id4',
  id5:'$id5'
 };
</script>
<script type=\"text/javascript\" src=\"http://data.nlpcaptcha.in/js/tagVisibility.js\"></script>"
?>
</div> 
<p id="par"></p>
<div style="height:50px"></div>

 
</body>
</html>