public class ErrorMessage {
    private Kubernetes kubernetes;

    public Kubernetes getKubernetes() {
        return kubernetes;
    }

    public void setKubernetes(Kubernetes kubernetes) {
        this.kubernetes = kubernetes;
    }

    public static class Kubernetes {
        private Container container;
        private String namespace;

        public Container getContainer() {
            return container;
        }

        public void setContainer(Container container) {
            this.container = container;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public static class Container {
            private String name;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }
        }
    }

    @Override
    public String toString() {
        return this.kubernetes.getNamespace() + " - " + this.getKubernetes().getContainer().getName();
    }
}
