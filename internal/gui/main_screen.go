package gui

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type mainModel struct {
	model  tea.Model
	styles *Styles
	lg     *lipgloss.Renderer
	width  int
}

func NewStyles(lg *lipgloss.Renderer) *Styles {
	s := Styles{}
	s.Base = lg.NewStyle().
		Padding(1, 4, 0, 1)
	s.HeaderText = lg.NewStyle().
		Foreground(indigo).
		Bold(true).
		Padding(0, 1, 0, 2)
	s.Status = lg.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(indigo).
		PaddingLeft(1).
		MarginTop(1)
	s.StatusHeader = lg.NewStyle().
		Foreground(green).
		Bold(true)
	s.Highlight = lg.NewStyle().
		Foreground(lipgloss.Color("212"))
	s.ErrorHeaderText = s.HeaderText.
		Foreground(red)
	s.Help = lg.NewStyle().
		Foreground(lipgloss.Color("240"))
	return &s
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func RootScreen() mainModel {
	var rootModel tea.Model

	menu_model := initialModel(nil)
	rootModel = &menu_model

	m := mainModel{width: maxWidth}
	m.model = rootModel
	m.lg = lipgloss.DefaultRenderer()
	m.styles = NewStyles(m.lg)

	return m
}

func (m mainModel) Init() tea.Cmd {
	return m.model.Init()
}

func (m mainModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = min(msg.Width, maxWidth) - m.styles.Base.GetHorizontalFrameSize()
	}
	return m.model.Update(msg)
}

func (m mainModel) View() string {
	s := m.styles

	const statusWidth = 20
	statusMarginLeft := m.width - statusWidth - lipgloss.Width(m.model.View()) - s.Status.GetMarginRight()
	status := s.Status.
		Height(lipgloss.Height(m.model.View())).
		Width(statusWidth).
		MarginLeft(statusMarginLeft).
		Render(s.StatusHeader.Render("Status") + "\n")

	header := m.appBoundaryView("Welcome to MAEP client")
	body := lipgloss.JoinHorizontal(lipgloss.Top, m.model.View(), status)
	return s.Base.Render(header + "\n" + body + "\n\n")
}

func (m mainModel) SwitchScreen(model tea.Model) (tea.Model, tea.Cmd) {
	m.model = model
	return m.model, m.model.Init()
}

func (m mainModel) appBoundaryView(text string) string {
	return lipgloss.PlaceHorizontal(
		m.width,
		lipgloss.Left,
		m.styles.HeaderText.Render(text),
		lipgloss.WithWhitespaceChars("/"),
		lipgloss.WithWhitespaceForeground(indigo),
	)
}
