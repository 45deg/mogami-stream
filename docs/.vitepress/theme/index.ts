import DefaultTheme from "vitepress/theme";
import type { Theme } from "vitepress";
import OperatorCatalog from "./components/OperatorCatalog.vue";
import SlotDiagram from "./components/SlotDiagram.vue";
import "./style.css";

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component("OperatorCatalog", OperatorCatalog);
    app.component("SlotDiagram", SlotDiagram);
  },
} satisfies Theme;
