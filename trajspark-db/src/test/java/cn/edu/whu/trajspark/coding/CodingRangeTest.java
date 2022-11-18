package cn.edu.whu.trajspark.coding;

import cn.edu.whu.trajspark.coding.sfc.SFCRange;
import junit.framework.TestCase;

/**
 * @author Haocheng Wang
 * Created on 2022/11/13
 */
public class CodingRangeTest extends TestCase {

  public void testConcatSfcRange() {
    CodingRange codingRange = new CodingRange();
    SFCRange sfcRange = new SFCRange(10, 10, true);
    codingRange.concatSfcRange(sfcRange);
    System.out.println(codingRange);
  }

//  public void testConcatPosCodeRange() {
//    PosCodeRange posCodeRange = new PosCodeRange();
//    posCodeRange.setLower(new PosCode((byte) 10));
//    posCodeRange.setUpper(new PosCode((byte) 15));
//    CodingRange codingRange = new CodingRange();
//    SFCRange sfcRange = new SFCRange(10, 10, true);
//    codingRange.concatSfcRange(sfcRange);
//    codingRange.concatPosCodeRange(posCodeRange);
//    System.out.println(codingRange);
//  }
}