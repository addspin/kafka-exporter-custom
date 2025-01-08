package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

var (
	consumerGroupLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_lag",
			Help: "Lag for each consumer group, topic and partition",
		},
		[]string{"consumergroup", "topic", "partition"},
	)

	consumerGroupLagSum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_lag_sum",
			Help: "Total lag for each consumer group and topic",
		},
		[]string{"consumergroup", "topic"},
	)

	consumerGroupCurrentOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_current_offset",
			Help: "Current offset for each consumer group, topic and partition",
		},
		[]string{"consumergroup", "topic", "partition"},
	)

	consumerGroupCurrentOffsetSum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_current_offset_sum",
			Help: "Current offset sum for each consumer group and topic",
		},
		[]string{"consumergroup", "topic"},
	)

	consumerGroupMembers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_members",
			Help: "Number of members in consumer group",
		},
		[]string{"consumergroup"},
	)

	topicPartitionCurrentOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_topic_partition_current_offset",
			Help: "Current offset of topic partition",
		},
		[]string{"topic", "partition"},
	)

	// Статусы групп
	stateValues = map[string]float64{
		"Unknown":             0,
		"PreparingRebalance":  1,
		"CompletingRebalance": 2,
		"Stable":              3,
		"Dead":                4,
		"Empty":               5,
	}

	consumerGroupHealth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_health",
			Help: "Consumer group state (0=UNKNOWN, 1=PREPARING_REBALANCE, 2=COMPLETING_REBALANCE, 3=STABLE, 4=DEAD, 5=EMPTY)",
		},
		[]string{"consumergroup", "state"},
	)
)

func init() {
	prometheus.MustRegister(consumerGroupLag)
	prometheus.MustRegister(consumerGroupLagSum)
	prometheus.MustRegister(consumerGroupCurrentOffset)
	prometheus.MustRegister(consumerGroupCurrentOffsetSum)
	prometheus.MustRegister(consumerGroupMembers)
	prometheus.MustRegister(topicPartitionCurrentOffset)
	prometheus.MustRegister(consumerGroupHealth)
}

func main() {
	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	scrapeInterval := viper.GetDuration("metrics.scrape_interval")
	retryInterval := viper.GetDuration("metrics.retry_interval")

	config := sarama.NewConfig()
	config.Version = sarama.V3_9_0_0

	// Настройка SASL PLAIN аутентификации
	config.Net.SASL.Enable = viper.GetBool("kafka.sasl.enabled")
	config.Net.SASL.Mechanism = sarama.SASLMechanism(viper.GetString("kafka.sasl.mechanism"))
	config.Net.SASL.User = viper.GetString("kafka.sasl.username")
	config.Net.SASL.Password = viper.GetString("kafka.sasl.password")

	// Указываем протокол безопасности
	config.Net.TLS.Enable = viper.GetBool("kafka.tls.enabled")
	config.Net.SASL.Handshake = viper.GetBool("kafka.sasl.handshake")

	// Таймауты для соединения с брокером клиентом
	config.Net.DialTimeout = viper.GetDuration("kafka.timeout.dial")
	config.Net.ReadTimeout = viper.GetDuration("kafka.timeout.read")
	config.Net.WriteTimeout = viper.GetDuration("kafka.timeout.write")

	// Адреса брокеров из вашего кластера
	brokers := viper.GetStringSlice("kafka.brokers")

	// Запускаем сбор метрик в отдельной горутине
	go func() {
		for {
			client, err := sarama.NewClient(brokers, config)
			if err != nil {
				log.Printf("Error creating client: %v", err)
				time.Sleep(retryInterval)
				continue
			}

			updateMetrics(client)
			client.Close()

			time.Sleep(scrapeInterval)
		}
	}()
	log.Println("Starting server on port", viper.GetInt("exporterPort"))
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(viper.GetInt("exporterPort")), nil))
}

func updateMetrics(client sarama.Client) {
	log.Printf("Starting metrics update...")

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Printf("Error creating admin client: %v", err)
		return
	}

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Printf("Error listing consumer groups: %v", err)
		return
	}

	for group := range groups {
		describeGroup, err := admin.DescribeConsumerGroups([]string{group})
		if err != nil {
			log.Printf("Error describing consumer group %s: %v", group, err)
			consumerGroupHealth.WithLabelValues(group, "Unknown").Set(stateValues["Unknown"])
			continue
		}

		if len(describeGroup) == 0 {
			log.Printf("No description received for group %s", group)
			consumerGroupHealth.WithLabelValues(group, "Unknown").Set(stateValues["Unknown"])
			continue
		}

		groupDesc := describeGroup[0]
		members := len(groupDesc.Members)
		consumerGroupMembers.WithLabelValues(group).Set(float64(members))

		state := groupDesc.State
		stateValue, exists := stateValues[state]
		if !exists {
			log.Printf("Unknown state %s for group %s", state, group)
			state = "Unknown"
			stateValue = stateValues["Unknown"]
		}
		consumerGroupHealth.WithLabelValues(group, state).Set(stateValue)

		offsets, err := admin.ListConsumerGroupOffsets(group, nil)
		if err != nil {
			log.Printf("Error getting offsets for group %s: %v", group, err)
			continue
		}

		for topic, partitions := range offsets.Blocks {
			var topicLagSum float64
			var topicCurrentOffsetSum float64

			for partition, offsetBlock := range partitions {
				currentOffset := offsetBlock.Offset
				latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Printf("Error getting latest offset: %v", err)
					continue
				}

				lag := latestOffset - currentOffset
				partitionStr := strconv.FormatInt(int64(partition), 10)

				consumerGroupLag.WithLabelValues(group, topic, partitionStr).Set(float64(lag))
				consumerGroupCurrentOffset.WithLabelValues(group, topic, partitionStr).Set(float64(currentOffset))

				topicPartitionCurrentOffset.WithLabelValues(topic, partitionStr).Set(float64(latestOffset))

				topicLagSum += float64(lag)
				topicCurrentOffsetSum += float64(currentOffset)
			}

			consumerGroupLagSum.WithLabelValues(group, topic).Set(topicLagSum)

			consumerGroupCurrentOffsetSum.WithLabelValues(group, topic).Set(topicCurrentOffsetSum)
		}
	}
	log.Printf("Metrics update completed successfully")
}
