<!DOCTYPE html>
<html>
<head>
<title>Twaler</title>
</head>
<body>
<?php
$user='snorgadmin';
$password='snorg321';
$database='twaler';
mysql_connect('localhost',$user,$password);
@mysql_select_db($database) or die('unable to select database');

if ($_GET['id']) {
  $id=$_GET['id'];
  $query="SELECT * FROM users WHERE user_id=$id";
  $result=mysql_query($query);
  $name=mysql_result($result, 0, 'user_name');
  $screen_name=mysql_result($result, 0, 'screen_name');
  $loc=mysql_result($result, 0, 'location');
  $desc=mysql_result($result, 0, 'description');
  $url=mysql_result($result, 0, 'url');
  $follower_cnt=mysql_result($result, 0, 'followers_count');
  $friend_cnt=mysql_result($result, 0, 'friends_count');
  $tweet_cnt=mysql_result($result, 0, 'status_count');
  echo "<h2>$name (@$screen_name)</h2>";
  echo "$desc<br>";
  echo "$loc<br>";
  echo "<a href=\"$url\">$url</a><br>";
  echo "tweets: $tweet_cnt<br>";
  echo "following: $friend_cnt<br>";
  echo "follower: $follower_cnt<br>";
  echo "<a href=\"#posts\">posts</a><br>";
  echo "<a href=\"#reads\">reads</a><br>";
  echo "<hr>";

  $query="SELECT * FROM tweets WHERE user_id=$id ORDER BY date DESC";
  $result=mysql_query($query);
  $num=mysql_numrows($result);
  echo "<a name='posts'> <i>displaying $num tweets</i></a><br>";
  $i=0;
  while ($i < $num) {
    $date=mysql_result($result, $i, "date");
    $tweet=mysql_result($result, $i, "text");

    echo "$date : $tweet<br>";

    $i++;
  }

  echo "<hr>";
  $query="SELECT * FROM tweets WHERE user_id IN (SELECT friend_id FROM friends WHERE user_id=$id) ORDER BY date DESC";
  $result=mysql_query($query);
  $num=mysql_numrows($result);
  $num=min($num, 1000);
  echo "<a name='reads'> <i>displaying the latest $num tweets</i></a><br>";
  $i=0;
  while ($i < $num) {
    $date=mysql_result($result, $i, "date");
    $tweet=mysql_result($result, $i, "text");

    echo "$date : $tweet<br>";

    $i++;
  }
} else {
  $query='SELECT * FROM users';
  $result=mysql_query($query);
  $num=mysql_numrows($result);
  echo "$num users<br>";
  $i=0;
  while ($i < $num) {
    $name=mysql_result($result, $i, "user_name");
    $id=mysql_result($result, $i, "user_id");

    echo "<a href=\"twaler.php?id=$id\">$name</a><br>";

    $i++;
  }
}

mysql_close();
?>
</body>
</html>
