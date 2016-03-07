package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.graph.FriendshipGraph;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;


public class RankerQuery2 extends StreamFunctionProcessor {
    private FriendshipGraph friendsGraph;
    private String iij_timestamp;
    private String ts;

    @Override
    protected Object[] process(Object[] objects) {
        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 6) {
            System.err.println("Required Parameters : Six");
            return null;
        }

        friendsGraph = new FriendshipGraph();

        List<Attribute> attributeList = new ArrayList<Attribute>();

        return attributeList;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[0];
    }

    public void restoreState(Object[] objects) {

    }
}
