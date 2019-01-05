package test.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZookeeperTest {
	private ZooKeeper zk = null;

	public ZookeeperTest() {
		try {
			zk = new ZooKeeper("localhost:2181", 500000, new Watcher() {
				// 监控所有被触发的事件
				public void process(WatchedEvent event) {
					System.out.println(event.getPath());
					System.out.println("zk动作:"+event.getType().name());
					// System.out.println(event.getState().getIntValue());
				}
			});
			zk.exists("/root/childone", true);// 观察这个节点发生的事件
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void createNodes() {
		try {
			// 创建一个节点root，数据是mydata,不进行ACL权限控制，节点为永久性的(即客户端shutdown了也不会消失)
			zk.create("/root", "mydata".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			
			// 在root下面创建一个childone znode,数据为childone,不进行ACL权限控制，节点为永久性的
			zk.create("/root/childone", "childone".getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 更新节点数据
	 *@author YMY 
	 *@date 2015年4月30日 下午3:55:25
	 */
	@Test
	public void updateNodes() {
		try {

			// 取得/root/childone节点下的数据,返回byte[]
			System.out.println(new String(zk.getData("/root/childone", true, null)));

			// 修改节点/root/childone下的数据，第三个参数为版本，如果是-1，那会无视被修改的数据版本，直接改掉
			zk.setData("/root/childone", "childonemodify2".getBytes(), -1);

			System.out.println(new String(zk.getData("/root/childone", true,
					null)));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void deleteNodes() {
		try {
			// 第二个参数为版本，－1的话直接删除，无视版本
			zk.delete("/root/childone", -1);
//			zk.delete("/root", -1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			ZookeeperTest zkTest = new ZookeeperTest();
			zkTest.createNodes();
			 zkTest.deleteNodes();
			zkTest.updateNodes();

			while (true) {
				Thread.sleep(1000);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}