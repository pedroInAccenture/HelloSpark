import org.scalatest.funsuite.AnyFunSuite
import sql.Operation

import java.util

class OperationTest extends AnyFunSuite {

  test("multiply zero by any other number is zero") {
    val op = new Operation()
    val res = op.mutiply(0,222)
    assert(res === 0)
  }

  test("multiply one by any other number is second one") {

    val op = new Operation()
    val second = 222
    val res = op.mutiply(1, second)
    assert(res === second)
  }

}
