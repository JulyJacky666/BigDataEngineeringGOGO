### Part 4: MapReduce on MovieLens Data
> Once the data are inserted into MongoDB, do the followings using MapReduce:
  Write a MapReduce to do the followings:
> - Number of Movies released per year (Movies Collection)
1. extract 4digit year information from title
2. use global variable  moviesCount and yearsCount to do the final average 
```javascript
var map = function(){
	var year = this.title.replace(/[^0-9]/ig,"");
	var year = year.substring(year.length-4,year.length);
	emit(year,1);
 	};

var reduce = function(year,counts){
	yearsCount +=1;
	mvCount = Array.sum(counts);
	moviesCount +=mvCount;
	return mvCount;
}
var final = function(key,reducedValue){
	return {movies:reducedValue,allAverage:moviesCount/yearsCount}
}

 db.HW1_PART4_MOVIES.mapReduce(map,reduce,{out:{inline:1},scope:{yearsCount:0,moviesCount:0}, finalize:final})
```
output:
```
> db.HW1_PART4_MOVIES.mapReduce(mapFN_year,reduce,{out:{inline:1},scope:{yearsCount:0,moviesCount:0}, finalize:final})
{
	"results" : [
		{
			"_id" : "",
			"value" : {
				"movies" : 18,
				"allAverage" : 160.06926605504586
			}
		},
		{
			"_id" : "1891",
			"value" : {
				"movies" : 1,
				"allAverage" : 160.06926605504586
			}
		},
		{
			"_id" : "1893",
			"value" : {
				"movies" : 1,
				"allAverage" : 160.06926605504586
			}
		}
```
> - Number of Movies per genre (Movies Collection)
1. at very least, we need to split each genre in the raw data and emit every  genre record
2. then do the same thing as before

```javascript
 var mapFn2 = function(){
	var genres = this.genres.split("|");
    for (var idx = 0; idx < genres.length; idx++) {
        var genres = genres[idx];
        emit(genres, 1);
	}	
 };

 var reduceFn2 = function(genre,counts){
	genresCount +=1;
	mvCount = Array.sum(counts);
	moviesCount +=mvCount;
	return mvCount;
}

var final = function(key,reducedValue){
	return {movies:reducedValue,allAverage:moviesCount/genresCount}
}

 db.HW1_PART4_MOVIES.mapReduce(mapFn2,reduceFn2,{out:{inline:1},scope:{genresCount:0,moviesCount:0},finalize:final })
```
result:
```
> db.HW1_PART4_MOVIES.mapReduce(mapFn2,reduceFn2,{out:{inline:1},scope:{genresCount:0,moviesCount:0},finalize:final })
{
	"results" : [
		{
			"_id" : "(no genres listed)",
			"value" : {
				"movies" : 246,
				"allAverage" : 1203.8397915988278
			}
		},
		{
			"_id" : "Action",
			"value" : {
				"movies" : 3520,
				"allAverage" : 1203.8397915988278
			}
		},
		{
			"_id" : "Adventure",
			"value" : {
				"movies" : 1357,
				"allAverage" : 1203.8397915988278
			}
		}

```
> - Number of Movies per rating (Ratings Collection)

```javascript
 var mapFn3 = function(){
	emit(this.movieId,1)	
 };

 var reduceFn3 = function(movies,ratings){
 	moviesCount +=1;

 	rtCounting = Array.sum(ratings);
 	ratingsCount +=rtCounting
 	return rtCounting
 };

 var final3 = function(key,reducedValue){
	return {movies:reducedValue,allAverage:ratingsCount/moviesCount}
}

 db.HW1_PART4_RATINGS.mapReduce(mapFn3,reduceFn3,{out:{inline:1},scope:{ratingsCount:0,moviesCount:0},finalize:final3 })
```
result(tooks long time):

> - Number of times each movie was tagged (Tags Collection) 

too simple

```javascript
db.HW1_PART4_TAGS.mapReduce(function(){emit(this.movieId,1)},function(key,values){return Array.sum(values)},{out:"MR_for_tags"})
```



### Part 5:
>Write a Java (could be a console app - will only run once to import the data into MongoDB) program to read the access.log file (attached), and insert
 into access collection.
 Once the data are inserted into MongoDB, do the followings using MapReduce:
 > - Number of times any webpage was visited by the same IP address.
 
 Solution:
 ```javascript
db.HW1_PART5_LOGFILE.mapReduce(function(){emit(this.content.split(" ")[0],1)},function(key,values){return Array.sum(values)},{out:{inline:1}})
```


 >- Number of times any webpage was visited each month 
 
 Solution:
  ```javascript
 db.HW1_PART5_LOGFILE.mapReduce(function(){emit(this.content.split(" ")[3].split("/")[1],1)},function(key,values){return Array.sum(values)},{out:{inline:1}})
 ```
 