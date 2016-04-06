package org.wso2.siddhi.debs2016.query;


import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.EventSenderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;
/**
 Query 2

 This query addresses the change of interests with large communities. It represents a version of query type 2 from the 2014 SIGMOD Programming contest.
 Unlike in the SIGMOD problem, the version for the DEBS Grand Challenge focuses on the dynamic change of query results over time, i.e., calls for a
 continuous evaluation of the results.

 Goal of the query:
 Given an integer k and a duration d (in seconds), find the k comments with the largest range, where the range of a comment is defined as the size
 of the largest connected component in the graph defined by persons who (a) have liked that comment (see likes, comments), (b) where the comment was created not more than d seconds ago, and (c) know each other (see friendships).

 Parameters: k, d

 Input Streams: likes, friendships, comments

        Output:

        The output includes a single timestamp ts and exactly k strings per line. The timestamp and the strings should be separated by commas.
        The k strings represent comments, ordered by range from largest to smallest, with ties broken by lexicographical ordering (ascending).
        The k strings and the corresponding timestamp must be printed only when some input triggers a change of the output, as defined above.
        If less than k strings can be determined, the character “-” (a minus sign without the quotations) should be printed in place of each missing string.

        The field ts corresponds to the timestamp of the input data item that triggered an output update. For instance, a new friendship relation may
        change the size of a community with a shared interest and hence may change the k strings. The timestamp of the event denoting the added friendship
        relation is then the timestamp ts for that line's output. Also, the output must be updated when the results change due to the progress of time, e.g.,
        when a comment is older that d. Specifically, if the update is triggered by an event leaving a time window at t2 (i.e., t2 = timestamp of the event +
        window size), the timestamp for the update is t2. As in Query 1, it is needless to say that timestamps refer to the logical time of the input data
        streams, rather than on the system clock.

        In summary, the output is specified as follows:

        ts: the timestamp of thetuple event that triggers a change in the output.
        comments_1,...,comment_k: top k comments ordered by range, starting with the largest range (comment_1).
        Sample output tuples for the query with k=3 could look as follows:

        2010-10-28T05:01:31.022+0000,I love strawberries,-,-
        2010-10-28T05:01:31.024+0000,I love strawberries,what a day!,-
        2010-10-28T05:01:31.027+0000,I love strawberries,what a day!,well done
        2010-10-28T05:01:31.032+0000,what a day!,I love strawberries,well done
**/
public class Query2 {

}
