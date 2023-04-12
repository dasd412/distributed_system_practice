import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class Application {

    //복제용 주소 집합 /? 클러스터 복제 세트의 이름
    public static final String MONGO_DB_URL = "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0";
    public static final String DB_NAME = "online-school";
    public static final double MIN_GPA = 90.0;

    public static void main(String[] args) {
        String courseName = args[0];
        String studentsName = args[1];
        int age = Integer.parseInt(args[2]);
        double gpa = Double.parseDouble(args[3]);

        MongoDatabase onlineSchoolDb = connectToMongoDB(MONGO_DB_URL, DB_NAME);

        enroll(onlineSchoolDb, courseName, studentsName, age, gpa);
    }

    private static void enroll(MongoDatabase database, String courseName, String studentsName, int age, double gpa) {

        if (!isValidCourse(database, courseName)) {
            System.out.println("Invalid course" + courseName);
            return;
        }

        MongoCollection<Document> collection = database.getCollection(courseName)
                .withWriteConcern(WriteConcern.MAJORITY)//<-클러스터 과반수에 완전히 복제
                .withReadPreference(ReadPreference.primary());//<-읽기 작업에서 엄격한 일관성 보장

        if (collection.find(eq("name", studentsName)).first() != null) {
            System.out.println("student" + studentsName + "already enrolled");
            return;
        }

        if (gpa < MIN_GPA) {
            System.out.println("please improve your grades");
            return;
        }

        collection.insertOne(new Document("name", studentsName).append("age", age)
                .append("gpa", gpa));

        System.out.println("student" + studentsName + "was enrolled in " + courseName);

        for (Document document : collection.find()) {
            System.out.println(document);
        }
    }

    private static boolean isValidCourse(MongoDatabase database, String courseName) {
        for (String collectionName : database.listCollectionNames()) {
            if (collectionName.equals(courseName)) {
                return true;
            }
        }
        return false;
    }

    public static MongoDatabase connectToMongoDB(String url, String dbName) {
        MongoClient mongoClient = new MongoClient(new MongoClientURI(url));
        return mongoClient.getDatabase(dbName);
    }
}
