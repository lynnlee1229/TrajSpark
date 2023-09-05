package cn.edu.whu.trajspark.controller.drop;

import cn.edu.whu.trajspark.database.Database;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class DropDataController {
  @ResponseBody
  @GetMapping (value = "/Drop")
  public String dropTableData(@RequestParam(value = "dataSetName") String dataSetName)
      throws IOException {
    Database instance = Database.getInstance();
    if(!instance.dataSetExists(dataSetName)){
      return "Dataset: " + dataSetName + " has not exist.";
    }
    instance.deleteDataSet(dataSetName);
    return "Dataset: " + dataSetName + " deleted.";
  }
}
