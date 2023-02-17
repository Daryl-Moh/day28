package ibf2022paf.day28.configs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import static ibf2022paf.day28.Constants.*;

@Configuration
public class AppConfig {

    @Value("${mongo.url}")
    private String mongoUrl;

    // @Primary
    // @Bean(name=BGG)
    // public MongoTemplate createBgg() {
    //     MongoClient client = MongoClients.create(mongoUrl);
    //     return new MongoTemplate(client, BGG);
    // }

    @Bean(name=SHOWS)
    public MongoTemplate createShows() {
        MongoClient client = MongoClients.create(mongoUrl);
        return new MongoTemplate(client, SHOWS);
    }
}
