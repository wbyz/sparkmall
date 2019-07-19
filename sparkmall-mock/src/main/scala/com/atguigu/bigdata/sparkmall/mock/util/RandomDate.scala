package com.atguigu.bigdata.sparkmall.mock.util

import java.util.{Date, Random}

/**
  * @author Witzel
  * @since 2019/7/14 22:13
  */
object RandomDate {

    def apply(startDate:Date,endDate:Date,step:Int): RandomDate ={
        val randomDate = new RandomDate()
        val avgStepTime = (endDate.getTime- startDate.getTime)/step
        randomDate.maxTimeStep=avgStepTime*2
        randomDate.lastDateTime=startDate.getTime
        randomDate
    }


    class RandomDate{
        var lastDateTime =0L
        var maxTimeStep=0L

        def  getRandomDate()={
            val timeStep = new Random().nextInt(maxTimeStep.toInt)
            lastDateTime = lastDateTime+timeStep

            new Date( lastDateTime)
        }
    }
}

