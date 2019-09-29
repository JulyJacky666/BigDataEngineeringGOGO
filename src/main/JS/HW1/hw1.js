db.HW1_games.insertMany([
    { name:"LOL", genre:"MOBA", rating:99 },
    { name:"DOTA", genre:"MOBA", rating:98 },
    { name:"WOW", genre:"RPG", rating:99 },
    { name:"PUBG", genre:"FPS", rating:96 },
    { name:"OW", genre:"FPS", rating:93 }
])


// part3
var mapFn1 = function(){
    var year = this.title.replace(/[^0-9]/ig,"");
    var year = year.substring(year.length-4,year.length);
    emit(year,1);
};

var reduceFn1 = function(year,counts){
    yearsCount +=1;
    mvCount = Array.sum(counts);
    moviesCount +=mvCount;
    return mvCount;
}
var final1 = function(key,reducedValue){
    return {movies:reducedValue,allAverage:moviesCount/yearsCount}
}

db.HW1_PART4_MOVIES.mapReduce(mapFn1,reduceFn1,{out:{inline:1},scope:{yearsCount:0,moviesCount:0}, finalize:final})



//genres
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

var final2 = function(key,reducedValue){
    return {movies:reducedValue,allAverage:moviesCount/genresCount}
}

db.HW1_PART4_MOVIES.mapReduce(mapFn2,reduceFn2,{out:{inline:1},scope:{genresCount:0,moviesCount:0},finalize:final })




//ratings

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

db.HW1_PART4_RATINGS.mapReduce(mapFn3,reduceFn3,{out:{inline:1},scope:{ratingsCount:0,moviesCount:0},finalize:final })



//for tags
db.HW1_PART4_TAGS.mapReduce(function(){emit(this.movieId,1)},function(key,values){return Array.sum(values)},{out:"MR_for_tags"})



//part 5:
db.HW1_PART5_LOGFILE.mapReduce(function(){emit(this.content.split(" ")[0],1)},function(key,values){return Array.sum(values)},{out:{inline:1}})
db.HW1_PART5_LOGFILE.mapReduce(function(){emit(this.content.split(" ")[3].split("/")[1],1)},function(key,values){return Array.sum(values)},{out:{inline:1}})



