package HW1;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class part4 {
    private static MongoClient client = MongoClients.create();

    public static void main(String[] args) throws Exception {
        CreateCollectionsAndLoadData();
        part5MRs();

    }
    // using mongo built in MR to do
    public static void part5MRs() throws Exception {


//
//        String map1 = "function(){\n" +
//                "\tvar year = this.title.replace(/[^0-9]/ig,\"\");\n" +
//                "\tvar year = year.substring(year.length-4,year.length);\n" +
//                "\temit(year,1);\n" +
//                " \t};";
//        String reduce1 = "function(year,counts){\n" +
//                "\tyearsCount +=1;\n" +
//                "\tmvCount = Array.sum(counts);\n" +
//                "\tmoviesCount +=mvCount;\n" +
//                "\treturn mvCount;\n" +
//                "}";
//
//        String final1 = "final = function(key,reducedValue){\n" +
//                "\treturn {movies:reducedValue,allAverage:moviesCount/yearsCount}\n" +
//                "}";
//        for (Document d : client.getDatabase("INFO7250").getCollection("HW1_PART4_MOVIES").mapReduce(map1, reduce1).limit(10)) {
//            System.out.println(d.toJson());
//        }
//
//        String mapFN_genre = "function(){\n" +
//                "\tvar genres = this.genres.split(\"|\");\n" +
//                "    for (var idx = 0; idx < genres.length; idx++) {\n" +
//                "        var genres = genres[idx];\n" +
//                "        emit(genres, 1);\n" +
//                "\t}\t\n" +
//                " };";
//        String reduceFn_genre = "function(key,values){return Array.sum(values)}";
////        for(Document d: client.getDatabase("INFO7250").getCollection("HW1_PART4_MOVIES").mapReduce(mapFN_genre,reduceFn_genre)){
////            System.out.println(d.toJson());
////        }
//
//        for (Document d : client.getDatabase("INFO7250").getCollection("HW1_PART4_TAGS").mapReduce("function(){emit(this.movieId,1)}", "function(key,values){return Array.sum(values)}")) {
//            System.out.println(d.toJson());
//        }


    }

    public static void CreateCollectionsAndLoadData() throws Exception {
        MongoDatabase db = client.getDatabase("INFO7250");
//        db.createCollection("HW1_PART4");
        MongoCollection movieCollection = db.getCollection("HW1_PART4_MOVIES");
        movieCollection.insertMany(new part4().loadCsvToMongoDB("movies.csv"));

        MongoCollection ratingCollection = db.getCollection("HW1_PART4_RATINGS");
        ratingCollection.insertMany(new part4().loadCsvToMongoDB("ratings.csv"));
        ratingCollection.insertMany(new part4().loadCsvToMongoDB("ratings2.csv"));

        MongoCollection tagCollection = db.getCollection("HW1_PART4_TAGS");
        tagCollection.insertMany(new part4().loadCsvToMongoDB("tags.csv"));

    }

    public List<Document> loadCsvToMongoDB(String path) throws Exception {
        File input = new File(path);
        CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
        CsvMapper csvMapper = new CsvMapper();
        List<Document> result = new ArrayList<Document>();
        List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(input).readAll();
        int i = 0;
        for (Object obj : readAll) {
            LinkedHashMap document = (LinkedHashMap) obj;
            Document newDocument = new Document();
            if (i % 3000 == 0)
                System.out.println("processing :" + i);

            for (Object key : document.keySet()) {
                newDocument.append(key.toString(), document.get(key));
            }
            result.add(newDocument);
            i++;
        }
        System.out.println("reading data done..");
        return result;

    }
}
