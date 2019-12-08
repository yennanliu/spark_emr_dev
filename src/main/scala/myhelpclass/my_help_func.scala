package  myhelpclass

object my_help_func {

  def my_sum_func (num1 : Int, num2 : Int):
    Int = {return num1 + num2 }

  def my_product_func (num1 : Int, num2 : Int):
    Int = { return num1*num2}

  def my_print_func (num1 : Int, var1 : String):
    String = {return "outut : " + var1}

  def my_no_return_value() : 
    Unit = println("this is my_func4") // use "Unit" if no return 

  def my_sum_func_with_dynamic_input (nums : Int*):
    Int = nums.reduceLeft((sum,num) => sum + num)

  }