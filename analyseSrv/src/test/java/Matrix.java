import org.apache.kafka.common.metrics.stats.Max;

import java.util.ArrayList;

/**
 * 该类主要用于保存特征信息
 * data : 主要是保存特征矩阵
 */
public class Matrix {

    public ArrayList<ArrayList<String>> data;

    public Matrix(){
        data = new ArrayList<>();
    }
}
