package es.unizar.tmdad.lab3.flows;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.amqp.Amqp;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.social.twitter.api.Tweet;

@Configuration
@Profile("fanout")
public class TwitterFlowTrend extends TwitterFlowCommon {

	final static String TWITTER_TREND_A_QUEUE_NAME = "twitter_trend_queue";

	@Autowired
	RabbitTemplate rabbitTemplate;
	
	@Autowired
	FanoutExchange fanoutExchange;
	
	@Bean
	Queue twitterTrendQueue() {
		return new Queue(TWITTER_TREND_A_QUEUE_NAME, false);
	}

	@Bean
	Binding twitterTrendBinding() {
		return BindingBuilder.bind(twitterTrendQueue()).to(fanoutExchange);
	}
	
	@Bean
	public DirectChannel requestChannelRabbitMQ() {
		return MessageChannels.direct().get();
	}

	@Bean
	public AmqpInboundChannelAdapter amqpInbound() {
		SimpleMessageListenerContainer smlc = new SimpleMessageListenerContainer(rabbitTemplate.getConnectionFactory());
		smlc.setQueues(twitterTrendQueue());
		return Amqp.inboundAdapter(smlc).outputChannel(requestChannelRabbitMQ()).get();
	}
	
	@Bean
	public IntegrationFlow sendTrends() {
		return IntegrationFlows.from(requestChannelRabbitMQ())
				.filter("payload instanceof T(org.springframework.social.twitter.api.Tweet)")
				.aggregate(agregacionSpec(), null)
				.transform(getDiezTrend())
				.handle("streamSendingService", "sendTrends").get();
	}

	private GenericTransformer<List<Tweet>, List<Map.Entry<String, Integer>>> getDiezTrend() {
		return list -> {
			Map<String, Integer> map = new HashMap<String, Integer>();
			list.stream().forEach(t ->{
				t.getEntities().getHashTags().forEach( h -> {
					Integer count = map.get(h.getText());
					if(count == null) map.put(h.getText(), 1);
					else map.put(h.getText(), count + 1);
				});
			});

			Comparator<Map.Entry<String, Integer>> comparator = 
				(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) -> 
					Integer.compare(entry1.getValue(), entry2.getValue());
			return map.entrySet().stream().sorted(comparator).limit(10).collect(Collectors.toList());
		};
	}

	private Consumer<AggregatorSpec> agregacionSpec(){
		return a -> a.correlationStrategy(m -> 1).releaseStrategy(g -> g.size() == 1000).expireGroupsUponCompletion(true);
	}
	
}