

object testList {
    def main(args: Array[String]) = {

        val list1 = Array((1,"test1"), (2,"test2"))
        val list2 = Array((3,"test3"), (4,"test4"))
        val list3 = list1 ++ list2
        list3.foreach(x => println(x._1, x._2 ))

    }
}