library(arrow)
demo_server <- load_flight_server("demo_flight_server")
server <- demo_server$DemoFlightServer(port = 8089)
server$serve()

client <- flight_connect(port = 8089)
# Upload some data to our server so there's something to demo
flight_put(client, iris, path = "test_data/iris")


library(arrow)
library(dplyr)
client <- flight_connect(port = 8089)
client %>%
  flight_get("test_data/iris") %>%
  group_by(Species) %>%
  summarize(max_petal = max(Petal.Length))
