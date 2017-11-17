import java.util.concurrent.*;

/**
 * Created by Administrator on 2017/11/14.
 */
public class ConcurrentTest {

    public static void main(String[] args) {

        //第一种方式
        ExecutorService executor = Executors.newCachedThreadPool();
        Task2 task = new Task2();
        executor.execute(task);
        System.out.println("主线程在执行任务");
        executor.shutdown();

        //第二种方式，注意这种方式和第一种方式效果是类似的，只不过一个使用的是ExecutorService，一个使用的是Thread
        /*Task task = new Task();
        FutureTask<Integer> futureTask = new FutureTask<Integer>(task);
        Thread thread = new Thread(futureTask);
        thread.start();*/
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        System.out.println("主线程执行完毕");
    }
}

class Task implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        System.out.println("子线程在进行计算");
        Thread.sleep(3000);
        int sum = 0;
        for(int i=0;i<100;i++)
            sum += i;
        return sum;
    }

}


class Task2 implements Runnable {

    @Override
    public void run() {
        System.out.println("子线程在进行计算");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int sum = 0;
        for(int i=0;i<100;i++)
            sum += i;
        System.out.println("子线程 sum: "+sum);
    }
}