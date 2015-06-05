package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal(b()*b() - 4 * a()* c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    
    def cs : Set[Double]= {
        val delta = computeDelta(a, b, c)()
        delta match {
          case v if v < 0 => Set()
          case _ => Set((-1.0 * b() + math.sqrt(delta))/(2*a()),(-1 * b() - math.sqrt(delta))/(2*a())) 
        }
      
    }
    Signal(cs)
  }
}
