package cn.edu.whu.trajspark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;

@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
public class TrajsparkVisApplication {

  public static void main(String[] args) {
    SpringApplication.run(TrajsparkVisApplication.class, args);
  }

}
