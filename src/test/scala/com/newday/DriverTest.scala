package com.newday

import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class DriverTest extends FlatSpec with Matchers with BeforeAndAfterEach{

  it should "raise an IllegalArgumentException when you are no providing the 3 arguments" in{
    val driver = Driver
    a[IllegalArgumentException] should be thrownBy driver.main(Array("",""))
  }


}
