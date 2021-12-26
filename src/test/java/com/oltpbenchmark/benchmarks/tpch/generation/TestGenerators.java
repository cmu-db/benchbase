import java.util.List;

import junit.framework.TestCase;

import com.oltpbenchmark.benchmarks.tpch.generation.*;

public class TestGenerators extends TestCase {

    public TestGenerators() {
        super();
    }

    public void testGenerators() {
        System.out.println("---REGION---");
        for (List<Object> elems : new RegionGenerator()) {
            for (Object elem : elems) {
                System.out.print(elem.toString());
                System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.println("");

        System.out.println("---NATION---");
        for (List<Object> elems : new NationGenerator()) {
            for (Object elem : elems) {
                System.out.print(elem.toString());
                System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.println("");

        System.out.println("---SUPPLIER---");
        for (List<Object> elems : new SupplierGenerator(0.001, 1, 1)) {
            for (Object elem : elems) {
                System.out.print(elem.toString());
                System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.println("");

        System.out.println("---CUSTOMER---");
        for (List<Object> elems : new CustomerGenerator(0.0001, 1, 1)) {
            for (Object elem : elems) {
                System.out.print(elem.toString());
                System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.println("");

        System.out.println("---PART---");
        for (List<Object> elems : new PartGenerator(0.0001, 1, 1)) {
            for (Object elem : elems) {
                System.out.print(elem.toString());
                System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.println("");

        System.out.println("---ORDER---");
        for (List<Object> elems : new OrderGenerator(0.00001, 1, 1)) {
            for (Object elem : elems) {
                System.out.print(elem.toString());
                System.out.print(" ");
            }
            System.out.print("\n");
        }
        System.out.println("");

        // System.out.println("---PARTSUPP---");
        // for (List<Object> elems : new PartSupplierGenerator(0.0001, 1, 1)) {
        //     for (Object elem : elems) {
        //         System.out.print(elem.toString());
        //         System.out.print(" ");
        //     }
        //     System.out.print("\n");
        // }
        // System.out.println("");

        // System.out.println("---LINEITEM---");
        // for (List<Object> elems : new LineItemGenerator(0.0001, 1, 1)) {
        //     for (Object elem : elems) {
        //         System.out.print(elem.toString());
        //         System.out.print(" ");
        //     }
        //     System.out.print("\n");
        // }
        // System.out.println("");
    }

}